# Creating a Dedicated Test Database in SQL Server Management Studio

The production schemas can be cloned into a standalone integration-test database by restoring a
backup under a new name. The following procedure keeps the test database isolated while still
mirroring production objects (tables, stored procedures, functions, etc.).

## 1. Capture a Production Backup
1. In SSMS connect to the production SQL Server instance.
2. Right-click the production database (e.g., `Intraday`) and choose **Tasks → Back Up…**.
3. Use the **Full** backup type and select a disk location that is accessible from the test
   environment.
4. Start the backup and wait for it to complete before moving on.

> :warning: Always confirm with your DBA/security team that taking a production backup is
> acceptable. When in doubt, request a sanitized copy prepared by the data team.

## 2. Restore the Backup Under a New Name
1. In SSMS connect to the target test SQL Server instance.
2. Right-click **Databases** and choose **Restore Database…**.
3. Select **Device** and browse to the `.bak` file created in the previous step.
4. In the **Destination** section change the database name to something like
   `IntradayTest`. This ensures you do not overwrite production accidentally.
5. In the **Files** page adjust the **Restore As** paths if required (for example to move
   MDF/LDF files onto test storage).
6. Click **OK** to start the restore. When it finishes you will have a new database that is a
   full copy of production.

## 3. Recreate or Remap Logins
1. Expand the restored database → **Security → Users** to confirm that the application login
   exists.
2. If the login is missing on the test server, run `CREATE LOGIN` on the instance and then
   `CREATE USER … FOR LOGIN …` inside the restored database.
3. Grant the same roles/permissions as production (usually `db_datareader`, `db_datawriter`,
   and execution rights on required stored procedures).

## 4. Update the Application Configuration
1. Choose a dedicated SQL credential for the test database.
2. Update `appsettings.json` (or environment-specific overrides) with the new connection string.
   With the code changes in this PR you can now place a connection string under
   `Database → Environments → Test → ConnectionString`:
   ```json
   "Database": {
     "ActiveEnvironment": "Test",
     "Environments": {
       "Test": {
         "ConnectionString": "Server=.;Database=IntradayTest;User Id=intraday_user;Password=ChangeMe!;TrustServerCertificate=True;"
       }
     }
   }
   ```
3. If you store credentials in AWS Secrets Manager instead of using an inline connection string,
   create a dedicated secret (for example `qq-intraday-test-credentials`) and place its name under
   `Database → Environments → Test → SecretName`.
4. Redeploy or restart the service so that the new configuration is picked up.

## 5. Keep the Test Database Current
- Schedule regular refreshes (nightly/weekly) by repeating the restore process or by running the
  same migration scripts that are applied in production.
- Use SQL Agent jobs or automation scripts to truncate/seed data that should differ from
  production (e.g., anonymise sensitive fields).
- Consider enabling `AUTO_CLOSE` = `OFF` and running `DBCC CHECKDB` periodically to detect issues
  early.

Following this workflow gives the test environment a clean, production-like schema without
polluting the primary database.
