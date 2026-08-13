import streamlit as st
import io, os
import pandas as pd
import pymysql
import datetime
from pandas.tseries.offsets import MonthEnd
from sqlalchemy import create_engine, text

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(layout="wide", page_title="실거래가 조회")

if 'result_df' not in st.session_state:
    st.session_state.result_df = None

# Tkinter 버튼 색상 재현을 위한 CSS
st.markdown("""
    <style>
    div.stButton > button:first-child {
        background-color: #FFFF00; color: #FF0000; font-weight: bold; 
        border: 1px solid red; height: 2em; width: 100%;
    }
    .status-bar {
        background-color: #f0f2f6; padding: 10px; border-radius: 5px;
        border: 1px solid #dcdcdc; margin-top: 20px;
    }
    </style>
""", unsafe_allow_html=True)



# --- 2. 법정동 데이터 로드 (캐싱 적용으로 속도 향상) ---
@st.cache_data
def load_location_data():        
    file_path = "file_content.txt"
    
    if not os.path.exists(file_path):
        st.error(f"'{file_path}' 파일을 찾을 수 없습니다.")
        return {}
    file_content = ""
    # 1. 인코딩 시도 (cp949는 윈도우 메모장 기본 한글 인코딩인 경우가 많음)
    try:
        with open(file_path, "r", encoding="cp949") as f:
            file_content = f.read()
    except UnicodeDecodeError:
        # 2. cp949 실패 시 utf-8로 다시 시도
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                file_content = f.read()
        except Exception as e:
            st.error(f"파일 인코딩 오류: {e}")
            return {}
       
    data = {}
    lines = file_content.strip().split('\n')
    for line in lines[1:]:
        parts = line.split('\t')
        if len(parts) < 3 or parts[2].strip() != '존재':
            continue        
        lawd_cd = parts[0].strip()[:5] # 시군구 코드 (앞 5자리)
        full_address = parts[1].strip()
        address_parts = full_address.split()
        
        if len(address_parts) < 2:
            continue
        sido = address_parts[0]
        
        # 시군구명 추출 로직 (원본 코드 복잡성 유지)
        sigungu = ""
        dong = ""        
        big_city = ['성남시','수원시','고양시','부천시','안양시','안산시','용인시','창원시','천안시','포항시','청주시','전주시','화성시']
        
        if len(address_parts) == 2 and address_parts[1] in big_city:
            continue
        elif len(address_parts) >= 2 and address_parts[0] =='세종특별자치시':
            sigungu = "세종시"
            dong = ' '.join(address_parts[1:])         
        elif len(address_parts) > 2 and address_parts[1] in big_city:
            sigungu = " ".join(address_parts[1:3])
            if len(address_parts) > 3:
                dong = ' '.join(address_parts[3:])
        else:
            sigungu = address_parts[1]
            if len(address_parts) > 2:
                dong = ' '.join(address_parts[2:])
        # 1. 시도 계층 구조 생성
        if sido not in data:
            data[sido] = {}
        if sigungu and sigungu not in data[sido]:
            data[sido][sigungu] = []
        if dong and dong not in data[sido][sigungu]:
            data[sido][sigungu].append(dong)           
        

    # 정렬
    for sido_val in data:
        for sigungu_val in data[sido_val]:
            data[sido_val][sigungu_val].sort()
        
        if sido not in data: data[sido] = {}
        if sigungu not in data[sido]: data[sido][sigungu] = []
        if dong and dong not in data[sido][sigungu]: data[sido][sigungu].append(dong)
    
    return data



sido_data = load_location_data()

# --- 3. 사이드바/상단: 검색 조건 설정 ---
#st.title("실거래 데이터 조회")
st.markdown('<h3 style="font-size: 18px;">실거래데이터 조회</h3>', unsafe_allow_html=True)

# URL 선택 (라디오 버튼)
URL_KEYS = ["분양권", "아파트 매매", "아파트 전월세", "오피스텔 매매", "오피스텔 전월세", "연립/다세대 매매", "연립/다세대 전월세"]
selected_type = st.radio("🔍 검색 항목 선택", URL_KEYS, horizontal=True, index=1)

# 입력 프레임 (기존 input_frame_2 재현)
with st.container():
    col1, col2, col3, col4, col5 = st.columns([1.2, 1.2, 1.2, 1.5, 1.5])
    
    with col1:
        sido = st.selectbox("시도", options=sorted(list(sido_data.keys())), index=8) # 8=서울
    with col2:
        sigungu_options = sorted(list(sido_data[sido].keys())) if sido in sido_data else []
        sigungu = st.selectbox("시군구", options=sigungu_options)
    with col3:
        dong_options = ["전체"] + sorted(sido_data[sido][sigungu]) if sigungu in sido_data.get(sido, {}) else ["전체"]
        dong = st.selectbox("읍면동", options=dong_options)
    with col4:
        sub_col1, sub_col2 = st.columns(2)
        ex_min = sub_col1.selectbox("전용(min)", [10, 20, 30, 40, 59, 84, 120], index=4)
        ex_max = sub_col2.selectbox("전용(max)", [60, 75, 85, 100, 120, 150, 200], index=2)
    with col5:
        default_date = (datetime.date.today() + MonthEnd(-2))
        deal_ymd = st.date_input("기준월(월말)>=", default_date)

# 조회 및 다운로드 버튼
btn_col, space, excel_col, etc_col = st.columns([1, 1, 1, 7])

with btn_col:
    search_clicked = st.button("🚀 조회", use_container_width=True)

# --- 4. 데이터 조회 로직 (조회 버튼 클릭 시 실행) ---
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 환경 변수 읽기
def get_engine():
    
    # 로컬 .env 또는 서버 환경 변수에서 가져옴
    db_host = st.secrets["DB_HOST"]
    db_user = st.secrets["DB_USER"]
    db_pw = st.secrets["DB_PASSWORD"]
    db_name = st.secrets["DB_NAME"]
    db_port = st.secrets["DB_PORT"]   
    
    # SQLAlchemy 엔진 생성
    db_url = f"mysql+pymysql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}"
    return create_engine(db_url)


if search_clicked:
    try:                         
        engine = get_engine()

        # 지역 그룹 정의
        sma = ['서울특별시', '인천광역시', '경기도']
        big6 = ['부산광역시', '대구광역시', '대전광역시', '광주광역시', '울산광역시', '세종특별자치시']
        dodo = ['강원특별자치도', '충청북도', '충청남도', '전라특별자치도', '전라남도', '경상북도', '경상남도', '제주특별자치도']
        
        table_map = {
            "분양권": "bunyang", "아파트 매매": "sale_sma", "아파트 전월세": "rent_sma",
            "오피스텔 매매": "ot_sale", "오피스텔 전월세": "ot_rent",
            "연립/다세대 매매": "villa_sale", "연립/다세대 전월세": "villa_rent"
        }

        # 테이블 분기 로직
        if selected_type == '아파트 매매':
            if sido in big6: table_name = 'sale_big6'
            elif sido in dodo: table_name = 'sale_dodo'
            else: table_name = 'sale_sma'
        elif selected_type == '아파트 전월세' and sido not in sma:
            table_name = 'rent_notsma'
        else:
            table_name = table_map.get(selected_type, "sale_sma")

        # 쿼리 및 파라미터 구성 (딕셔너리 바인딩 방식)
        query = f"SELECT * FROM {table_name} WHERE 광역시도 = :sido AND 시자치구 = :sigungu AND 기준월 >= :deal_ymd"
        params = {
            "sido": sido, "sigungu": sigungu, 
            "deal_ymd": deal_ymd.strftime('%Y-%m-%d'),
            "ex_min": ex_min, "ex_max": ex_max
        }
        
        if dong != "전체":
            query += " AND 법정동 = :dong"
            params["dong"] = dong
        query += " AND 전용면적 >= :ex_min AND 전용면적 <= :ex_max LIMIT 5000"

        with st.spinner('테이블 조회 중...'):
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
        
        # 데이터 정제 및 세션 저장
        if not df.empty:
            df.drop('id', axis=1, inplace=True)                
            st.session_state.result_df = df.reset_index(drop=True)
        else:
            st.session_state.result_df = pd.DataFrame() # 빈 결과 저장        
        engine.dispose()

    except Exception as e:
        st.error(f"조회 중 오류 발생: {e}")

# --- 5. 결과 출력 (세션 상태를 확인하여 상시 유지) ---
if st.session_state.result_df is not None:
    df = st.session_state.result_df
    
    if not df.empty:
        st.dataframe(df, use_container_width=True, height=500, hide_index=True)
        
        # 검색건수 표시
        st.markdown(f"""
            <div class="status-bar">
                <span style='font-size: 16px; font-weight: bold;'>📊 검색 결과: </span>
                <span style='font-size: 26px; color: blue; font-weight: bold;'>{len(df):,}건</span>
            </div>
        """, unsafe_allow_html=True)

        # 엑셀 다운로드 버튼 (제일 오른쪽에 배치)
        with excel_col:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            buffer.seek(0)
            
            st.download_button(
                label="📥 엑셀 다운로드",
                data=buffer,
                file_name=f"{selected_type}_{deal_ymd}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    else:

        st.warning("조회된 데이터가 없습니다. 기준월을 과거 날짜로 변경해 보세요.")







