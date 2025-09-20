import os, math, json
from typing import List, Dict, Any, Tuple
import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from sklearn.linear_model import LogisticRegression

ES_HOST  = os.getenv("ES_HOST", "http://es:9200").rstrip("/")
ES_INDEX = os.getenv("ES_INDEX", "games")
BATCH    = int(os.getenv("BATCH_SIZE", "500"))
THRESH   = float(os.getenv("SUCCESS_RATING_THRESH", "4.5"))
MIN_S    = int(os.getenv("MIN_SAMPLES", "100"))

# ستون هایی که لازم داریم
FIELDS = [
    "title","description","rating","ratings_count",
    "feature_flags","genre","store","success_score"
]

es = Elasticsearch(ES_HOST, request_timeout=60)

def scan_docs() -> List[Dict[str, Any]]:
    rows = []
    for hit in helpers.scan(es, index=ES_INDEX, query={"query": {"match_all": {}}, "_source": FIELDS}, size=1000):
        src = hit.get("_source", {})
        rows.append({
            "_id": hit["_id"],
            "title": src.get("title"),
            "description": src.get("description"),
            "rating": src.get("rating"),
            "ratings_count": src.get("ratings_count"),
            "feature_flags": src.get("feature_flags") or [],
            "genre": src.get("genre"),
            "store": src.get("store"),
            "success_score": src.get("success_score")
        })
    return rows

def build_frame(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # اطمینان از انواع
    for col in ["rating","ratings_count","success_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # فیچرهای دسته‌ای: feature_flags (multi), genre, store
    df["feature_flags"] = df["feature_flags"].apply(lambda x: x if isinstance(x, list) else [])

    # one-hot برای feature_flags
    flags_ex = df[["_id","feature_flags"]].explode("feature_flags")
    flags_ex["feature_flags"] = flags_ex["feature_flags"].fillna("")
    flags_pv = pd.crosstab(flags_ex["_id"], flags_ex["feature_flags"])
    if "" in flags_pv.columns:
        flags_pv = flags_pv.drop(columns=[""])

    # dummies برای genre و store
    genre_dm = pd.get_dummies(df["genre"].fillna("unknown"), prefix="genre")
    store_dm = pd.get_dummies(df["store"].fillna("unknown"), prefix="store")

    # assemble features
    X = pd.concat([
        flags_pv.reindex(df["_id"]).fillna(0).astype(int).reset_index(drop=True),
        genre_dm.reset_index(drop=True),
        store_dm.reset_index(drop=True)
    ], axis=1)

    # لیبل موفقیت: rating >= THRESH
    y = (df["rating"].fillna(-1) >= THRESH).astype(int)

    return pd.concat([df[["_id","rating","ratings_count","success_score"]].reset_index(drop=True), X], axis=1).assign(label=y.values)

def train_model(df: pd.DataFrame) -> Tuple[Any, List[str]]:
    # فقط روی ردیف‌هایی که rating داریم آموزش می‌دیم
    train_mask = df["rating"].notna()
    Xtrain = df.loc[train_mask].drop(columns=["_id","rating","ratings_count","success_score","label"])
    ytrain = df.loc[train_mask, "label"].astype(int)

    # شرط حداقل برای آموزش
    if len(Xtrain) < MIN_S or ytrain.nunique() < 2:
        return None, list(Xtrain.columns)

    # کلاس‌های نامتوازن → وزن‌دهی متعادل
    model = LogisticRegression(max_iter=200, class_weight="balanced", solver="liblinear")
    try:
        model.fit(Xtrain, ytrain)
    except Exception:
        return None, list(Xtrain.columns)
    return model, list(Xtrain.columns)

def minmax(series: pd.Series) -> pd.Series:
    s = series.copy()
    s = s.fillna(0)
    lo, hi = s.min(), s.max()
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        # همه یکسان یا نامعتبر
        return pd.Series(np.zeros(len(s)), index=s.index)
    return (s - lo) / (hi - lo)

def predict_and_build_updates(df: pd.DataFrame, model, feat_cols: List[str]) -> List[Dict[str, Any]]:
    Xall = df[feat_cols].fillna(0)
    if model is not None:
        try:
            proba = model.predict_proba(Xall)[:,1]
        except Exception:
            proba = np.zeros(len(Xall))
    else:
        proba = np.zeros(len(Xall))

    # نرمال کردن success_score (اگر هست)
    norm_ss = minmax(df["success_score"]) if "success_score" in df.columns else pd.Series(np.zeros(len(df)))
    feature_score = 0.7 * proba + 0.3 * norm_ss.values

    updates = []
    ts_version = "v2"
    for _id, ps, fs in zip(df["_id"].values, proba, feature_score):
        doc = {
            "predicted_success": float(np.round(ps, 6)),
            "feature_score":     float(np.round(fs, 6)),
            "features_version":  ts_version
        }
        updates.append({
            "_op_type": "update",
            "_index": ES_INDEX,
            "_id": _id,
            "doc": doc,
            "doc_as_upsert": True
        })
    return updates

def main():
    rows = scan_docs()
    if not rows:
        print("[ANALYZER] no docs.")
        return
    df = build_frame(rows)
    if df.empty:
        print("[ANALYZER] empty dataframe.")
        return
    model, feat_cols = train_model(df)

    # چاپ اثر فیچرها (اگر مدل داریم)
    if model is not None:
        coefs = model.coef_[0]
        eff = sorted(zip(feat_cols, coefs), key=lambda t: abs(t[1]), reverse=True)[:20]
        print("[ANALYZER] top effects (coef):")
        for name, c in eff:
            print(f"  {name:32s}  {c:+.4f}")

    updates = predict_and_build_updates(df, model, feat_cols)
    ok, fail = helpers.bulk(es, updates, raise_on_error=False, request_timeout=120, chunk_size=BATCH)
    print(f"[ANALYZER] bulk ok={ok}, fail={len(fail) if isinstance(fail, list) else 0}")

if __name__ == "__main__":
    main()
