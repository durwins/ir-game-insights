import os
from typing import List, Dict, Any, Tuple
from elasticsearch import Elasticsearch, helpers
import pandas as pd
import numpy as np
import joblib

ES_URL   = os.getenv("ES_HOST", "http://es:9200").rstrip("/")
ES_INDEX = os.getenv("ES_INDEX", "games")
MODEL_DIR = os.getenv("MODEL_DIR", "/models")
BATCH = int(os.getenv("SCORE_BATCH","500"))

es = Elasticsearch(ES_URL, request_timeout=60)

def scan_ids_and_src() -> List[Dict[str,Any]]:
    fields = [
        "genre","rating","ratings_count",
        "feature_flags","assets_screenshot_count","assets_icon_count"
    ]
    q = {"query":{"match_all":{}}, "_source": fields}
    out=[]
    for h in helpers.scan(es, index=ES_INDEX, query=q, size=1000, preserve_order=False):
        out.append({"_id": h["_id"], **(h.get("_source", {}))})
    return out

def prepare_features(rows,
                     ohe,
                     top_genres,
                     top_flags,
                     num_columns: List[str],
                     feature_columns: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.DataFrame(rows)
    for c in ["genre","rating","ratings_count","feature_flags","assets_screenshot_count","assets_icon_count"]:
        if c not in df.columns: df[c] = np.nan

    # cast/clean
    df["ratings_count"] = pd.to_numeric(df["ratings_count"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["assets_screenshot_count"] = pd.to_numeric(df["assets_screenshot_count"], errors="coerce").fillna(0).astype(float)
    df["assets_icon_count"] = pd.to_numeric(df["assets_icon_count"], errors="coerce").fillna(0).astype(float)
    df["feature_flags"] = df["feature_flags"].apply(lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [x]))
    df["genre"] = df["genre"].fillna("unknown").astype(str).str.lower()
    df["genre_clipped"] = df["genre"].where(df["genre"].isin(top_genres), other="__other__")

    # numeric
    X_num = pd.DataFrame({
        "rating": df["rating"].fillna(0.0).astype(float),
        "log_ratings_count": np.log1p(df["ratings_count"].fillna(0).astype(float)),
        "assets_screenshot_count": df["assets_screenshot_count"].astype(float),
        "assets_icon_count": df["assets_icon_count"].astype(float),
    }, index=df.index)

    # categorical (use *trained* OHE)
    X_cat_arr = ohe.transform(df[["genre_clipped"]])
    cat_names = [f"genre__{g}" for g in ohe.categories_[0]]
    X_cat = pd.DataFrame(X_cat_arr, columns=cat_names, index=df.index)

    # flags (multi-hot) restricted to top_flags from training
    X_flags = pd.DataFrame(
        {f"flag__{f}": df["feature_flags"].apply(lambda L: 1 if f in (L or []) else 0) for f in top_flags},
        index=df.index
    ) if top_flags else pd.DataFrame(index=df.index)

    # concat
    X = pd.concat([X_num, X_cat, X_flags], axis=1)

    # ❶ اضافه کردن ستون‌های جاافتاده (در صورت نبودن در داده‌ی جدید)
    for col in feature_columns:
        if col not in X.columns:
            X[col] = 0

    # ❷ حذف ستون‌های اضافه که در آموزش نبودند
    extra = [c for c in X.columns if c not in feature_columns]
    if extra:
        X = X.drop(columns=extra)

    # ❸ مرتب‌سازی دقیق مطابق آموزش
    X = X[feature_columns]

    # ❹ نوع عددی و بدون NaN
    X = X.apply(pd.to_numeric, errors="coerce").fillna(0)

    return X, df

def main():
    artifact = joblib.load(os.path.join(MODEL_DIR, "model.pkl"))
    model = artifact["model"]
    ohe = artifact["ohe_genres"]
    top_genres = artifact["top_genres"]
    top_flags = artifact["top_flags"]
    num_columns = artifact["num_columns"]
    feature_columns = artifact["feature_columns"]  # ⟵ ترتیب نهایی ستون‌ها از آموزش

    rows = scan_ids_and_src()
    if not rows:
        print("[SCORE] no docs.")
        return

    X, df = prepare_features(rows, ohe, top_genres, top_flags, num_columns, feature_columns)

    # پیش‌بینی
    proba = model.predict_proba(X)[:, 1]

    # feature_score مکمل: نسبت فلگ‌های حاضر به کل top_flags
    if top_flags:
        fs = df["feature_flags"].apply(lambda L: len([f for f in (L or []) if f in top_flags]) / max(1, len(top_flags)))
    else:
        fs = pd.Series(0.0, index=df.index)
    fs = fs.fillna(0.0).astype(float)

    updates=[]
    for doc_id, p, sc in zip([r["_id"] for r in rows], proba, fs):
        updates.append({
            "_op_type":"update",
            "_index": ES_INDEX,
            "_id": doc_id,
            "doc": {
                "predicted_success": float(round(p,6)),
                "feature_score": float(round(sc,6)),
            },
            "doc_as_upsert": True
        })
        if len(updates) >= BATCH:
            helpers.bulk(es, updates, raise_on_error=False, request_timeout=120)
            updates.clear()
    if updates:
        helpers.bulk(es, updates, raise_on_error=False, request_timeout=120)

    print("[SCORE] done. wrote predicted_success & feature_score")

if __name__ == "__main__":
    main()
