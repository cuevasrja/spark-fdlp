-- CREATE TABLE albums ([id], [name], [album_group], [album_type], [release_date], [popularity])
-- Number of rows: 4820754

-- CREATE TABLE artists ([name], [id], [popularity], [followers])
-- Number of rows: 1066031

-- CREATE TABLE audio_features ([id], [acousticness], [analysis_url], [danceability], [duration], [energy], [instrumentalness], [key], [liveness], [loudness], [mode], [speechiness], [tempo], [time_signature], [valence])
-- Number of rows: 8740043

-- CREATE TABLE genres ([id])
-- Number of rows: 5489

-- CREATE TABLE r_albums_artists ([album_id], [artist_id])
-- Number of rows: 921486

-- CREATE TABLE r_albums_tracks ([album_id], [track_id])
-- Number of rows: 9900173

-- CREATE TABLE r_artist_genre ([genre_id], [artist_id])
-- Number of rows: 487386

-- CREATE TABLE r_track_artist ([track_id], [artist_id])
-- Number of rows: 11840402

-- CREATE TABLE tracks ([id], [disc_number], [duration], [explicit], [audio_feature_id], [name], [preview_url], [track_number], [popularity], [is_playable])
-- Number of rows: 8741672

-- Create a new table with the same schema as the old one but with the required constraints (albums)

BEGIN TRANSACTION;

ALTER TABLE "albums" RENAME TO "_albums_old";

CREATE TABLE albums (
    "id" TEXT PRIMARY KEY,
    "name" TEXT,
    "album_group" TEXT,
    "album_type" TEXT,
    "release_date" TEXT,
    "popularity" INTEGER
);

INSERT INTO albums ("id", "name", "album_group", "album_type", "release_date", "popularity") SELECT "id", "name", "album_group", "album_type", "release_date", "popularity" FROM "_albums_old";

DROP TABLE "_albums_old";

COMMIT;

-- Create a new table with the same schema as the old one but with the required constraints (artists)

BEGIN TRANSACTION;

ALTER TABLE "artists" RENAME TO "_artists_old";

CREATE TABLE artists (
    "name" TEXT,
    "id" TEXT PRIMARY KEY,
    "popularity" INTEGER,
    "followers" INTEGER
);

INSERT INTO artists ("name", "id", "popularity", "followers") SELECT "name", "id", "popularity", "followers" FROM "_artists_old";

DROP TABLE "_artists_old";

COMMIT;

-- Create a new table with the same schema as the old one but with the required constraints (audio_features)

BEGIN TRANSACTION;

ALTER TABLE "audio_features" RENAME TO "_audio_features_old";

CREATE TABLE audio_features (
    "id" TEXT PRIMARY KEY,
    "acousticness" REAL,
    "analysis_url" TEXT,
    "danceability" REAL,
    "duration" INTEGER,
    "energy" REAL,
    "instrumentalness" REAL,
    "key" INTEGER,
    "liveness" REAL,
    "loudness" REAL,
    "mode" INTEGER,
    "speechiness" REAL,
    "tempo" REAL,
    "time_signature" INTEGER,
    "valence" REAL
);

INSERT INTO audio_features ("id", "acousticness", "analysis_url", "danceability", "duration", "energy", "instrumentalness", "key", "liveness", "loudness", "mode", "speechiness", "tempo", "time_signature", "valence") SELECT "id", "acousticness", "analysis_url", "danceability", "duration", "energy", "instrumentalness", "key", "liveness", "loudness", "mode", "speechiness", "tempo", "time_signature", "valence" FROM "_audio_features_old";

DROP TABLE "_audio_features_old";

COMMIT;

-- Create a new table with the same schema as the old one but with the required constraints (genres)

BEGIN TRANSACTION;

ALTER TABLE "genres" RENAME TO "_genres_old";

CREATE TABLE genres (
    "id" TEXT PRIMARY KEY
);

INSERT INTO genres ("id") SELECT "id" FROM "_genres_old";

DROP TABLE "_genres_old";

COMMIT;

-- Create a new table with the same schema as the old one but with the required constraints (r_albums_artists)

BEGIN TRANSACTION;

ALTER TABLE "r_albums_artists" RENAME TO "_r_albums_artists_old";

CREATE TABLE r_albums_artists (
    "album_id" TEXT,
    "artist_id" TEXT,
    FOREIGN KEY ("album_id") REFERENCES "albums" ("id") ON DELETE CASCADE,
    FOREIGN KEY ("artist_id") REFERENCES "artists" ("id") ON DELETE CASCADE
);

INSERT INTO r_albums_artists ("album_id", "artist_id") SELECT "album_id", "artist_id" FROM "_r_albums_artists_old";

DROP TABLE "_r_albums_artists_old";

COMMIT;

-- Create a new table with the same schema as the old one but with the required constraints (r_albums_tracks)

BEGIN TRANSACTION;

ALTER TABLE "r_albums_tracks" RENAME TO "_r_albums_tracks_old";

CREATE TABLE r_albums_tracks (
    "album_id" TEXT,
    "track_id" TEXT,
    FOREIGN KEY ("album_id") REFERENCES "albums" ("id") ON DELETE CASCADE,
    FOREIGN KEY ("track_id") REFERENCES "tracks" ("id") ON DELETE CASCADE
);

INSERT INTO r_albums_tracks ("album_id", "track_id") SELECT "album_id", "track_id" FROM "_r_albums_tracks_old";

DROP TABLE "_r_albums_tracks_old";

COMMIT;

-- Create a new table with the same schema as the old one but with the required constraints (r_track_artist)

BEGIN TRANSACTION;

ALTER TABLE "r_artist_genre" RENAME TO "_r_artist_genre_old";

CREATE TABLE r_artist_genre (
    "genre_id" TEXT,
    "artist_id" TEXT,
    FOREIGN KEY ("genre_id") REFERENCES "genres" ("id") ON DELETE CASCADE,
    FOREIGN KEY ("artist_id") REFERENCES "artists" ("id") ON DELETE CASCADE
);

INSERT INTO r_artist_genre ("genre_id", "artist_id") SELECT "genre_id", "artist_id" FROM "_r_artist_genre_old";

DROP TABLE "_r_artist_genre_old";

COMMIT;

-- Create a new table with the same schema as the old one but with the required constraints (r_track_artist)

BEGIN TRANSACTION;

ALTER TABLE "r_track_artist" RENAME TO "_r_track_artist_old";

CREATE TABLE r_track_artist (
    "track_id" TEXT,
    "artist_id" TEXT,
    FOREIGN KEY ("track_id") REFERENCES "tracks" ("id") ON DELETE CASCADE,
    FOREIGN KEY ("artist_id") REFERENCES "artists" ("id") ON DELETE CASCADE
);

INSERT INTO r_track_artist ("track_id", "artist_id") SELECT "track_id", "artist_id" FROM "_r_track_artist_old";

DROP TABLE "_r_track_artist_old";

COMMIT;

-- Create a new table with the same schema as the old one but with the required constraints (tracks)

BEGIN TRANSACTION;

ALTER TABLE "tracks" RENAME TO "_tracks_old";

CREATE TABLE tracks (
    "id" TEXT PRIMARY KEY,
    "disc_number" INTEGER,
    "duration" INTEGER,
    "explicit" INTEGER,
    "audio_feature_id" TEXT,
    "name" TEXT,
    "preview_url" TEXT,
    "track_number" INTEGER,
    "popularity" INTEGER,
    "is_playable" INTEGER,
    FOREIGN KEY ("audio_feature_id") REFERENCES "audio_features" ("id") ON DELETE CASCADE
);

INSERT INTO tracks ("id", "disc_number", "duration", "explicit", "audio_feature_id", "name", "preview_url", "track_number", "popularity", "is_playable") SELECT "id", "disc_number", "duration", "explicit", "audio_feature_id", "name", "preview_url", "track_number", "popularity", "is_playable" FROM "_tracks_old";

DROP TABLE "_tracks_old";

COMMIT;