<?php
// This file is designed to be run from command line
// so we can do things like trigger via CRON.
if(!defined('__ROOT__'))
    define('__ROOT__', dirname(dirname(__FILE__)));

// Get DB and global info.
require_once(__ROOT__.'/init.php'); 

// Display all errors
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

// These configs are used to setup Equalify's database and execution.
require __DIR__ . '/vendor/autoload.php';
$dotenv = Dotenv\Dotenv::createImmutable(__DIR__, '.env');
$dotenv->safeLoad();

// Database creds
$db_host = $_ENV['DB_HOST'];
$db_port = $_ENV['DB_PORT']; 
$db_name = $_ENV['DB_NAME'];
$db_user = $_ENV['DB_USERNAME'];
$db_pass = $_ENV['DB_PASSWORD']; 

// Create DB connection
$pdo = new PDO("mysql:host=$db_host;port=$db_port;dbname=$current_db", "$db_user", "$db_pass");
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

// Start session
session_start();