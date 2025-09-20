import os, json
from typing import List, Dict, Any
from elasticsearch import Elasticsearch, helpers
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report
import joblib

ES_URL   = os.getenv("ES_HOST", "http://es:9200").rstrip("/")
ES_INDEX = os.getenv("ES_INDEX", "games")
MODEL_DIR = os.getenv("MODEL_DIR", "/models")
TOP_K_GENRES = int(os.getenv("TOP_K_GENRES","20"))
TOP_K_FLAGS  = int(os.getenv("TOP_K_FLAGS","40"))

es = Elasticsearch(ES_URL, request_timeout=60)

def scan_games() -> List[Dict[str, Any]]:
    fields = [
        "title","genre","rating","ratings_count",
        "feature_flags","assets_screenshot_count","assets_icon_count"
    ]
    q = {"query":{"match_all":{}}, "_source": fields}
    rows = []
    for h in helpers.scan(es, index=ES_INDEX, query=q, size=1000, preserve_order=False):
        rows.append(h.get("_source", {}))
    return rows

def prepare_dataframe(rows: List[Dict[str,Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for c in ["genre","rating","ratings_count","feature_flags","assets_screenshot_count","assets_icon_count"]:
        if c not in df.columns: df[c] = np.nan

    df["ratings_count"] = pd.to_numeric(df["ratings_count"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["assets_screenshot_count"] = pd.to_numeric(df["assets_screenshot_count"], errors="coerce").fillna(0).astype(float)
    df["assets_icon_count"] = pd.to_numeric(df["assets_icon_count"], errors="coerce").fillna(0).astype(float)
    df["feature_flags"] = df["feature_flags"].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))
    df["genre"] = df["genre"].fillna("unknown").astype(str).str.lower()
    return df

def label_success(df: pd.DataFrame) -> pd.Series:
    rc = df["ratings_count"].fillna(0)
    r  = df["rating"].fillna(0)
    # معیار ساده‌ی موفقیت (می‌تونی بعداً دقیق‌ترش کنی)
    y = ((r >= 4.4) & (rc >= 100)) | ((rc == 0) & (r >= 4.6))
    return y.astype(int)

def flags_to_frame(flags_col: pd.Series, top_flags: List[str]) -> pd.DataFrame:
    data = {f"flag__{f}": flags_col.apply(lambda L: 1 if f in (L or []) else 0) for f in top_flags}
    return pd.DataFrame(data)

def main():
    print("[TRAIN] fetching data from ES ...")
    rows = scan_games()
    if not rows:
        print("[TRAIN] no data found.")
        return

    df = prepare_dataframe(rows)
    print(f"[TRAIN] rows: {len(df)}")
    y = label_success(df)

    # top genres + clip
    top_genres = df["genre"].value_counts().head(TOP_K_GENRES).index.tolist()
    df["genre_clipped"] = df["genre"].where(df["genre"].isin(top_genres), other="__other__")

    # one-hot برای ژانر
    # نکته: برای سازگاری، از sparse=False استفاده می‌کنیم
    ohe = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    X_cat_arr = ohe.fit_transform(df[["genre_clipped"]])
    cat_names = [f"genre__{g}" for g in ohe.categories_[0]]
    X_cat = pd.DataFrame(X_cat_arr, columns=cat_names, index=df.index)

    # top feature flags
    all_flags = pd.Series([f for L in df["feature_flags"] for f in (L or [])])
    top_flags = all_flags.value_counts().head(TOP_K_FLAGS).index.tolist()
    X_flags = flags_to_frame(df["feature_flags"], top_flags)

    # numeric feats
    X_num = pd.DataFrame({
        "rating": df["rating"].fillna(0.0).astype(float),
        "log_ratings_count": np.log1p(df["ratings_count"].fillna(0).astype(float)),
        "assets_screenshot_count": df["assets_screenshot_count"].astype(float),
        "assets_icon_count": df["assets_icon_count"].astype(float),
    }, index=df.index)
    num_columns = list(X_num.columns)

    # concat
    X = pd.concat([X_num, X_cat, X_flags], axis=1)

    # ذخیره‌ی «ترتیب» فیچرها
    feature_columns = list(X.columns)

    # split + train
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)

    # eval
    p = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, p)
    print(f"[TRAIN] ROC-AUC: {auc:.3f}")
    print("[TRAIN] report:\n", classification_report(y_test, (p >= 0.5).astype(int), digits=3))

    # save artifacts
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump({
        "model": model,
        "ohe_genres": ohe,
        "top_genres": top_genres,
        "top_flags": top_flags,
        "num_columns": num_columns,
        "feature_columns": feature_columns,  # ⟵ مهم: ترتیب نهایی ستون‌ها
    }, os.path.join(MODEL_DIR, "model.pkl"))
    print(f"[TRAIN] saved model to {MODEL_DIR}/model.pkl")

if __name__ == "__main__":
    main()
