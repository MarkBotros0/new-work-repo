-- Initialize database for POS Transaction Ade
-- This script runs when MySQL container starts for the first time

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS postrxade CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Use the database
USE postrxade;

-- Create user for application (if not exists)
CREATE USER IF NOT EXISTS 'posuser'@'%' IDENTIFIED BY 'pospass';
GRANT ALL PRIVILEGES ON postrxade.* TO 'posuser'@'%';

-- Flush privileges
FLUSH PRIVILEGES;

-- Set timezone
SET time_zone = '+00:00';

-- Create tables will be handled by Hibernate/JPA
-- This is just for initial setup
