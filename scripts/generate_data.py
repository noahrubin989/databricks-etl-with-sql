"""
Generate fake data for a fictional language-learning app.

Outputs (in ./data):
    learners.csv                    -> loaded into source.learners (managed table)
    courses/courses.csv             -> ingested by bronze.courses (Streaming Table)
    lesson_attempts/batch_01.csv    -> ingested by bronze.lesson_attempts (Streaming Table)
    lesson_attempts/batch_02.csv    -> upload LATER (Step 6) to show incremental refresh
    lesson_attempts/batch_03.csv    -> upload LATER (Step 7) to show TRIGGER ON UPDATE

About 1.5% of lesson attempts are deliberately "bad" (invalid scores, missing IDs,
etc.) so the quality expectations in Step 2 have something to catch.

Uses only the Python standard library. Seeded, so every run produces identical files.

Usage (from the repo root):
    python scripts/generate_data.py
"""

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 42
OUT_DIR = Path(__file__).resolve().parent.parent / "data"

NUM_LEARNERS = 200
BAD_ROW_RATE = 0.015

# (country, native_language)
COUNTRIES = [
    ("Australia", "English"),
    ("New Zealand", "English"),
    ("England", "English"),
    ("Ireland", "English"),
    ("South Africa", "Afrikaans"),
    ("India", "Hindi"),
    ("Japan", "Japanese"),
    ("France", "French"),
    ("Argentina", "Spanish"),
    ("Spain", "Spanish"),
    ("Brazil", "Portuguese"),
    ("Italy", "Italian"),
    ("Germany", "German"),
    ("Fiji", "Fijian"),
    ("South Korea", "Korean"),
]

FIRST_NAMES = [
    "Liam", "Olivia", "Noah", "Ava", "Jack", "Mia", "Aroha", "Tane", "Siosaia", "Mele",
    "Arjun", "Priya", "Rohan", "Ananya", "Haruto", "Yui", "Sora", "Hina", "Lucas", "Chloe",
    "Mateo", "Sofia", "Santiago", "Valentina", "Thiago", "Ana", "Giulia", "Marco", "Lena",
    "Felix", "Ji-woo", "Min-jun", "Seo-yeon", "Pieter", "Annika", "Sean", "Aoife", "Ella",
    "Tom", "Grace", "Isla", "Finn", "Ruby", "Leo", "Zara", "Kai", "Maya", "Eli", "Nina", "Sam",
]

LAST_NAMES = [
    "Smith", "Williams", "Brown", "Taylor", "Walker", "Ngata", "Parata", "Vunipola",
    "Naivalu", "Sharma", "Patel", "Iyer", "Sato", "Suzuki", "Tanaka", "Martin", "Dubois",
    "Garcia", "Fernandez", "Lopez", "Silva", "Santos", "Rossi", "Bianchi", "Muller",
    "Schmidt", "Kim", "Park", "Lee", "van der Merwe", "Botha", "Murphy", "OBrien",
    "Kelly", "Jones", "Wilson", "Thompson", "Davies", "Evans", "Clarke",
]

# language -> (course code prefix, levels offered)
LANGUAGES = {
    "Spanish":    ("ES", ["A1", "A2", "B1", "B2"]),
    "French":     ("FR", ["A1", "A2", "B1", "B2"]),
    "Japanese":   ("JA", ["A1", "A2", "B1"]),
    "Italian":    ("IT", ["A1", "A2", "B1"]),
    "German":     ("DE", ["A1", "A2", "B1"]),
    "Portuguese": ("PT", ["A1", "A2"]),
    "Korean":     ("KO", ["A1", "A2"]),
    "Mandarin":   ("ZH", ["A1", "A2"]),
}

LEVEL_THEMES = {
    "A1": "First Steps",
    "A2": "Everyday Conversations",
    "B1": "Travel and Culture",
    "B2": "Confident Communication",
}

DEVICES = ["ios", "android", "web"]

# (filename, first day, last day, number of rows)
BATCHES = [
    ("batch_01.csv", date(2026, 9, 1), date(2026, 10, 31), 5000),
    ("batch_02.csv", date(2026, 11, 1), date(2026, 11, 15), 1000),
    ("batch_03.csv", date(2026, 11, 16), date(2026, 11, 21), 300),
]


def make_learners(rng):
    learners = []
    for learner_id in range(1, NUM_LEARNERS + 1):
        country, native_language = rng.choice(COUNTRIES)
        signup = date(2025, 1, 1) + timedelta(days=rng.randint(0, 600))
        learners.append({
            "learner_id": learner_id,
            "first_name": rng.choice(FIRST_NAMES),
            "last_name": rng.choice(LAST_NAMES),
            "country": country,
            "native_language": native_language,
            "signup_date": signup.isoformat(),
            "subscription_tier": rng.choices(["free", "premium"], weights=[70, 30])[0],
        })
    return learners


def make_courses(rng):
    courses = []
    for language, (code, levels) in LANGUAGES.items():
        for level in levels:
            courses.append({
                "course_id": f"{code}-{level}",
                "language": language,
                "level": level,
                "course_name": f"{language} {level}: {LEVEL_THEMES[level]}",
                "total_lessons": rng.choice([20, 25, 30, 40]),
            })
    return courses


def assign_enrolments(rng, learners, courses):
    """Each learner studies 1-2 courses, never in their native language."""
    enrolments = {}
    for learner in learners:
        options = [c for c in courses if c["language"] != learner["native_language"]]
        enrolments[learner["learner_id"]] = rng.sample(options, k=rng.choice([1, 1, 2]))
    return enrolments


def random_timestamp(rng, start, end):
    day = start + timedelta(days=rng.randint(0, (end - start).days))
    return datetime(day.year, day.month, day.day,
                    rng.randint(6, 23), rng.randint(0, 59), rng.randint(0, 59))


def make_good_attempt(rng, attempt_id, learners, activity, enrolments, start, end):
    learner = rng.choices(learners, weights=activity)[0]
    course = rng.choice(enrolments[learner["learner_id"]])
    score = max(0, min(100, int(rng.gauss(75, 15))))
    return {
        "attempt_id": attempt_id,
        "learner_id": learner["learner_id"],
        "course_id": course["course_id"],
        "lesson_number": rng.randint(1, course["total_lessons"]),
        "attempted_at": random_timestamp(rng, start, end).isoformat(sep=" "),
        "score": score,
        "minutes_spent": rng.randint(3, 25),
        "completed": "true" if score >= 60 else "false",
        "device": rng.choice(DEVICES),
    }


def make_bad(rng, row):
    """Corrupt a good row in one of several realistic ways."""
    problem = rng.choice([
        "missing_learner", "score_too_high", "negative_score",
        "negative_minutes", "future_date", "unknown_learner",
    ])
    if problem == "missing_learner":
        row["learner_id"] = ""
    elif problem == "score_too_high":
        row["score"] = rng.choice([101, 150, 999])
    elif problem == "negative_score":
        row["score"] = rng.choice([-1, -20])
    elif problem == "negative_minutes":
        row["minutes_spent"] = rng.choice([-5, -30])
    elif problem == "future_date":
        row["attempted_at"] = "2027-06-15 09:00:00"
    elif problem == "unknown_learner":
        row["learner_id"] = 9999  # no such learner in learners.csv
    return row


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows):>5} rows -> {path.relative_to(OUT_DIR.parent)}")


def main():
    rng = random.Random(SEED)

    learners = make_learners(rng)
    courses = make_courses(rng)
    enrolments = assign_enrolments(rng, learners, courses)
    # Some learners are far more active than others
    activity = [rng.choice([1, 2, 4, 8]) for _ in learners]

    write_csv(OUT_DIR / "learners.csv", learners)
    write_csv(OUT_DIR / "courses" / "courses.csv", courses)

    attempt_id = 1
    for filename, start, end, count in BATCHES:
        rows = []
        for _ in range(count):
            row = make_good_attempt(rng, attempt_id, learners, activity, enrolments, start, end)
            if rng.random() < BAD_ROW_RATE:
                row = make_bad(rng, row)
            rows.append(row)
            attempt_id += 1
        write_csv(OUT_DIR / "lesson_attempts" / filename, rows)


if __name__ == "__main__":
    main()
