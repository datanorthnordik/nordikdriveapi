CREATE TABLE IF NOT EXISTS roles (
    id SERIAL PRIMARY KEY,
    role VARCHAR(100) NOT NULL UNIQUE,
    priority INT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    firstname VARCHAR(100) NOT NULL,
    lastname VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password TEXT NOT NULL,
    role VARCHAR(100) NOT NULL DEFAULT 'User'
        REFERENCES roles(role) ON DELETE CASCADE,
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

CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    level VARCHAR(20) NOT NULL,
    service VARCHAR(50) NOT NULL,
    user_id INT NULL,
    action VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS file_edit_request (
    request_id SERIAL PRIMARY KEY,

    user_id INT NOT NULL
        REFERENCES users(id)
        ON DELETE CASCADE,

    status VARCHAR(50) NOT NULL DEFAULT 'pending',

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS file_edit_request_details (
    id SERIAL PRIMARY KEY,
    request_id INT NOT NULL REFERENCES file_edit_request(request_id) ON DELETE CASCADE,

    file_id INT NOT NULL REFERENCES file(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,

    row_id INT NOT NULL,                 -- e.g., 15048 in your example
    field_name VARCHAR(255) NOT NULL,    -- like "Parents Names"
    old_value TEXT,
    new_value TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX idx_logs_user_id ON logs(user_id);
CREATE INDEX idx_logs_service ON logs(service);
CREATE INDEX idx_logs_created_at ON logs(created_at);


