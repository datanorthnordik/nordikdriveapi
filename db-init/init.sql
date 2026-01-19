CREATE TABLE IF NOT EXISTS roles (
    id SERIAL PRIMARY KEY,
    role VARCHAR(100) NOT NULL UNIQUE,
    priority INT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    firstname VARCHAR(100) NOT NULL,
    lastname  VARCHAR(100) NOT NULL,
    email     VARCHAR(100) UNIQUE NOT NULL,
    password  TEXT NOT NULL,
    role      VARCHAR(100) NOT NULL DEFAULT 'User'
      REFERENCES roles(role) ON DELETE CASCADE,
    community TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE otps (
    id BIGINT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    code VARCHAR(6) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    INDEX idx_email (email)
);




CREATE TABLE IF NOT EXISTS file (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) UNIQUE NOT NULL,
    inserted_by INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    private BOOLEAN DEFAULT FALSE,
    community_filter BOOLEAN DEFAULT FALSE,
    is_delete BOOLEAN DEFAULT FALSE,
    size DECIMAL NOT NULL,
    version INT DEFAULT 1 NOT NULL,
    rows INT NOT NULL
);

CREATE TABLE IF NOT EXISTS file_version (
    id SERIAL PRIMARY KEY,
    file_id INT NOT NULL REFERENCES file(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    inserted_by INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    private BOOLEAN DEFAULT FALSE,
    is_delete BOOLEAN DEFAULT FALSE,
    size DECIMAL NOT NULL,
    version INT DEFAULT 1 NOT NULL,
    rows INT NOT NULL
);

CREATE TABLE IF NOT EXISTS file_data (
    id SERIAL PRIMARY KEY,
    file_id INT NOT NULL REFERENCES file(id) ON DELETE CASCADE,
    row_data JSONB NOT NULL,
    inserted_by INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    version INT DEFAULT 1 NOT NULL
);

CREATE TABLE IF NOT EXISTS file_access (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    file_id INT NOT NULL REFERENCES file(id) ON DELETE CASCADE,
    CONSTRAINT unique_user_file UNIQUE (user_id, file_id)
)

-- CREATE TABLE IF NOT EXISTS community (
--     id SERIAL PRIMARY KEY,
--     community_name VARCHAR(255) NOT NULL UNIQUE,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- ); 

-- CREATE TABLE IF NOT EXISTS access (
--     id SERIAL PRIMARY KEY,
--     community_name VARCHAR(255) NOT NULL REFERENCES community(community_name) ON DELETE CASCADE,
--     filename VARCHAR(255) REFERENCES file(filename) ON DELETE CASCADE,
--     user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     UNIQUE (community_name, filename, user_id)
-- );

-- ALTER TABLE access
-- ADD COLUMN status VARCHAR(255) NOT NULL DEFAULT 'pending',
-- ADD CONSTRAINT status_check CHECK (status IN ('pending', 'approved', 'rejected'));

-- CREATE TABLE IF NOT EXISTS user_roles (
--     id SERIAL PRIMARY KEY,
--     role VARCHAR(100) NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
--     user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
--     community_name VARCHAR(255)
-- );


-- CREATE INDEX IF NOT EXISTS idx_file_data_file_id ON file_data(file_id);

INSERT INTO roles (role, priority)
VALUES
    ('Admin',        1),
    ('User',         2);

-- INSERT INTO community (community_name) VALUES
-- ('Shoal Lake 40 First Nation'),
-- ('Listuguj Mi''gmaq First Nation'),
-- ('Tobique First Nation'),
-- ('Walpole Island First Nation'),
-- ('Mathias Colomb Cree Nation'),
-- ('Moose Cree First Nation'),
-- ('Chapleau Cree First Nation'),
-- ('Garden River First Nation'),
-- ('Hiawatha First Nation'),
-- ('Lac La Croix First Nation'),
-- ('Membertou First Nation'),
-- ('Siksika Nation'),
-- ('Sandy Lake First Nation'),
-- ('Alderville First Nation'),
-- ('Ahousaht First Nation'),
-- ('Wikwemikong Unceded Territory'),
-- ('Timmins, ON'),
-- ('Sault Ste. Marie, ON'),
-- ('Wabaseemoong Independent Nations'),
-- ('Six Nations of the Grand River'),
-- ('Bingwi Neyaashi Anishinaabek'),
-- ('Curve Lake First Nation'),
-- ('Fort William First Nation'),
-- ('Mississaugas of the Credit'),
-- ('Kettle and Stony Point First Nation'),
-- ('Samson Cree Nation'),
-- ('Grassy Narrows First Nation'),
-- ('Muskoday First Nation'),
-- ('Sagkeeng First Nation');

CREATE TABLE IF NOT EXISTS logs (
    id SERIAL PRIMARY KEY,
    level VARCHAR(20) NOT NULL,
    service VARCHAR(50) NOT NULL,
    user_id INT NULL,
    action VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB NULL,

    -- ✅ optional fields
    filename VARCHAR(255) NULL,
    communities TEXT[] NULL,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE file_edit_request (
    request_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    firstname VARCHAR(100),
    lastname VARCHAR(100),
    consent BOOLEAN DEFAULT FALSE,
    archive_consent BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    row_id INT NULL,
    is_edited BOOLEAN DEFAULT TRUE,
    file_id INT NOT NULL REFERENCES file(id) ON DELETE CASCADE,
    approved_by VARCHAR(255) REFERENCES users(id) ON DELETE SET NULL,
    community TEXT[] NOT NULL DEFAULT '{}'::text[],
    uploader_community TEXT[] NOT NULL DEFAULT '{}'::text[]
);



CREATE TABLE file_edit_request_photos (
     id SERIAL PRIMARY KEY,
    request_id INT NOT NULL,

    photo_url TEXT NOT NULL,
    file_name TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,

    is_gallery_photo BOOLEAN DEFAULT FALSE,
    is_approved BOOLEAN DEFAULT FALSE,
    approved_by VARCHAR(255),
    approved_at TIMESTAMP,

    row_id INT,
    file_id INT NOT NULL REFERENCES file(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT NOW(),

    document_type VARCHAR(20) NOT NULL DEFAULT 'photos',
    document_category VARCHAR(50) NULL,  
);



CREATE TABLE IF NOT EXISTS file_edit_request_details (
    id SERIAL PRIMARY KEY,
    request_id INT NOT NULL REFERENCES file_edit_request(request_id) ON DELETE CASCADE,

    file_id INT NOT NULL REFERENCES file(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,

    row_id INT NULL,                
    field_name VARCHAR(255) NOT NULL,
    old_value TEXT,
    new_value TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX idx_logs_user_id ON logs(user_id);
CREATE INDEX idx_logs_service ON logs(service);
CREATE INDEX idx_logs_created_at ON logs(created_at);

-- Communities table --

CREATE TABLE communities (
    id SERIAL PRIMARY KEY,
    community_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    approved BOOLEAN NOT NULL DEFAULT TRUE,
);

-- Query for inserting all unique communities --
INSERT INTO communities (community_name)
SELECT DISTINCT row_data ->> 'First Nation/Community'
FROM public.file_data
WHERE file_id = 40
  AND row_data ? 'First Nation/Community'
  AND row_data ->> 'First Nation/Community' IS NOT NULL;


CREATE INDEX IF NOT EXISTS idx_file_edit_request_community_gin
  ON file_edit_request USING GIN (community);

CREATE INDEX IF NOT EXISTS idx_file_edit_request_uploader_community_gin
  ON file_edit_request USING GIN (uploader_community);



