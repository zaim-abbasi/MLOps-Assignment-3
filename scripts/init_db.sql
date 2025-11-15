-- Initialize PostgreSQL database for NASA APOD data
-- This script creates the necessary table structure

CREATE TABLE IF NOT EXISTS apod_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    title VARCHAR(500) NOT NULL,
    url TEXT NOT NULL,
    explanation TEXT,
    media_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on date for faster queries
CREATE INDEX IF NOT EXISTS idx_apod_date ON apod_data(date);

-- Add comment to table
COMMENT ON TABLE apod_data IS 'NASA Astronomy Picture of the Day data';

