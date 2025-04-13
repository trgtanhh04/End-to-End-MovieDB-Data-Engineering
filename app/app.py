import streamlit as st
import pandas as pd
import psycopg2 
from psycopg2 import sql
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(dotenv_path='/home/tienanh/End-to-End Movie Recommendation/.env')

st.set_page_config(
    page_title="Movies Recommendation System",
    page_icon="🎬",
    layout="wide"
)

def get_db_connection():
    """Create a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            sslmode="require"
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None


def fetch_movie_recommendation(genre=None, release_year=None, 
                               language=None, score=None, 
                               runtime_minutes=None):
    """Fetch data from the database using the provided SQL query."""
    conn = get_db_connection()
    if conn:
        try:
            base_query = """
                SELECT 
                    mi.movie_id,
                    mi.name,
                    mi.release_year,
                    mi.language,
                    mi.score,
                    mi.runtime_minutes,
                    mi.status,
                    mi.budget,
                    mi.revenue,
                    mi.image,
                    mi.overview,
                    di.director_name,
                    mg.genre
                FROM public.movie_info mi
                JOIN public.movie_genres mg ON mi.movie_id = mg.movie_id
                JOIN public.director_info di ON mi.movie_id = di.movie_id
            """
            conditions = []
            if genre:
                conditions.append("mg.genre = %s")
            if release_year:
                conditions.append("mi.release_year = %s")
            if language:
                conditions.append("mi.language = %s")
            if score:
                conditions.append("mi.score >= %s")
            if runtime_minutes:
                conditions.append("mi.runtime_minutes <= %s")

            if conditions:
                base_query += " WHERE " + " AND ".join(conditions)
            base_query += " ORDER BY mi.release_year DESC"

            params = []
            if genre:
                params.append(genre)
            if release_year:
                params.append(release_year)
            if language:
                params.append(language)
            if score:
                params.append(score)
            if runtime_minutes:
                params.append(runtime_minutes)

            with conn.cursor() as cursor:
                cursor.execute(base_query, params)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(results, columns=columns)

                if not df.empty:
                    df['runtime_minutes'] = df['runtime_minutes'].apply(lambda x: round(x))
                    df['image'] = df['image'].apply(lambda x: x.replace("http://", "https://"))
                    df['release_year'] = pd.to_datetime(df['release_year'], format='%Y').dt.year
                    return df
                else:
                    # st.warning("Không có dữ liệu phù hợp với tiêu chí tìm kiếm.")
                    return pd.DataFrame(columns=columns)
        except Exception as e:
            st.error(f"Lỗi khi lấy dữ liệu: {e}")
        finally:
            conn.close()
    return pd.DataFrame()


def main():
    st.title("🎬 Movie Recommendation System")
    st.write("Find your next favorite movie!")
    st.sidebar.header("Filter Options")

    # Sidebar filters
    available_genres = [
        "Phim Hình Sự", "Phim Hành Động", "Phim Hài", "Phim Hoạt Hình", 
        "Phim Gia Đình", "Phim Khoa Học Viễn Tưởng", "Phim Phiêu Lưu", 
        "Phim Chính Kịch", "Phim Gây Cấn", "Phim Kinh Dị", "Phim Lãng Mạn", 
        "Phim Giả Tượng"
    ]
    genre = st.sidebar.selectbox("Chọn Thể Loại Phim", available_genres)
    release_year = st.sidebar.number_input("Release Year", min_value=1922, max_value=datetime.now().year, step=1)
    language = st.sidebar.selectbox("Chọn Ngôn Ngữ", ["Tiếng Anh", "Tiếng Hàn", "Tiếng Pháp", "Tiếng Việt", "Tiếng Tây Ban Nha", "Tiếng Nhật", "Cantonese"])
    score = st.sidebar.slider("Điểm Đánh Giá", 0, 100, 0)
    runtime_minutes = st.sidebar.number_input("Thời lượng (giờ)", min_value=0, step=1)

    # Fetch recommendations
    if st.sidebar.button("Get Recommendations"):
        with st.spinner("Đang tìm phim..."):
            recommendations = fetch_movie_recommendation(
                genre=genre,
                release_year=release_year,
                language=language,
                score=score,
                runtime_minutes=runtime_minutes,
            )
            st.session_state["recommendations"] = recommendations
            st.session_state["selected_movie"] = None

    # Display recommendations or details
    if "selected_movie" in st.session_state and st.session_state["selected_movie"] is not None:
        idx = st.session_state["selected_movie"]
        movie = st.session_state["recommendations"].iloc[idx]
        st.subheader(f"📋 Chi tiết phim: {movie['name']}")
        st.image(movie['image'], width=200)
        st.write(f"**Thể loại**: {movie['genre']}")
        st.write(f"**Năm phát hành**: {movie['release_year']}")
        st.write(f"**Ngôn ngữ**: {movie['language']}")
        st.write(f"**Điểm đánh giá**: {movie['score']}/100")
        st.write(f"**Thời lượng**: {movie['runtime_minutes']} giờ")
        st.write(f"**Đạo diễn**: {movie['director_name']}")
        st.write(f"**Nội dung**: {movie['overview']}")
        if st.button("Quay về"):
            st.session_state["selected_movie"] = None
    elif "recommendations" in st.session_state and not st.session_state.get("selected_movie"):
        recommendations = st.session_state["recommendations"]

        if not recommendations.empty:
            st.write("### 🎥 Gợi ý phim cho bạn")
            cols = st.columns(5)

            for idx, row in recommendations.iterrows():
                with cols[idx % 5]:
                    st.image(row['image'], caption=row['name'], use_container_width=True)
                    if st.button("Xem chi tiết", key=f"details_btn_{idx}"):
                        st.session_state["selected_movie"] = idx
                        st.rerun()

        else:
            st.warning("Không tìm thấy phim phù hợp với tiêu chí. Vui lòng thay đổi tiêu chí lọc và thử lại.")

if __name__ == "__main__":
    main()