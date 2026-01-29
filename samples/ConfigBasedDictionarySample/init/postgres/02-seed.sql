-- Seed data for reference tables

INSERT INTO countries (id, name, iso_code, region, population, is_active) VALUES
    (1, 'United States', 'US', 'North America', 331000000, TRUE),
    (2, 'Canada', 'CA', 'North America', 38000000, TRUE),
    (3, 'United Kingdom', 'GB', 'Europe', 67000000, TRUE),
    (4, 'Germany', 'DE', 'Europe', 83000000, TRUE),
    (5, 'France', 'FR', 'Europe', 67000000, TRUE),
    (6, 'Japan', 'JP', 'Asia', 126000000, TRUE),
    (7, 'Australia', 'AU', 'Oceania', 26000000, TRUE),
    (8, 'Brazil', 'BR', 'South America', 214000000, TRUE),
    (9, 'India', 'IN', 'Asia', 1400000000, TRUE),
    (10, 'China', 'CN', 'Asia', 1400000000, TRUE);

INSERT INTO currencies (code, name, symbol, decimal_places) VALUES
    ('USD', 'US Dollar', '$', 2),
    ('EUR', 'Euro', '\u20ac', 2),
    ('GBP', 'British Pound', '\u00a3', 2),
    ('JPY', 'Japanese Yen', '\u00a5', 0),
    ('CAD', 'Canadian Dollar', 'C$', 2),
    ('AUD', 'Australian Dollar', 'A$', 2),
    ('CNY', 'Chinese Yuan', '\u00a5', 2),
    ('INR', 'Indian Rupee', '\u20b9', 2),
    ('BRL', 'Brazilian Real', 'R$', 2);
