# Processor Test

This is a way to test the Equalify processor.

To get started:
1. Install your favorite PHP localhost - I love https://ddev.com 
2. Run `composer install` - or `ddev composer install`
3. Create a `.env` file:
    ```
    ## Configure database.
    DB_HOST='equalify-db'
    DB_USERNAME='root'
    DB_PASSWORD='root'
    DB_NAME='test'
    DB_PORT='3306'
    ```
4. Run `php install.php`
5. Run `php process_scans.php`
6. Log issues.