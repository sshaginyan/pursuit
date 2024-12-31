CREATE TABLE IF NOT EXISTS places (
    name                 TEXT,
    state_abbr           TEXT,
    lat                  REAL,
    "long"               REAL,
    pop_estimate_2022    INTEGER,
    county_fips          INTEGER,
    division             INTEGER,
    full_name            TEXT,
    place_fips           INTEGER,
    sum_lev              INTEGER,
    lsadc                INTEGER,
    display_name         TEXT,
    address              TEXT,
    url                  TEXT,
    highest_score        REAL,
    contact_place_id     TEXT
);