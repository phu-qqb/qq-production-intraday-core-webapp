# SQL Scripts

This folder contains utility scripts for managing the Wakett and intraday database schema.

## Refreshing test tables from production

The [`RefreshTestTables.sql`](./RefreshTestTables.sql) script truncates each test table and reloads it from the production equivalent so that the duplicated environment stays in sync. It:

- Validates that every production and test table exists before it begins copying, creating an empty target table automatically when the schema has not yet been provisioned.
- Preserves identity values when the target table has an identity column.
- Runs inside a single transaction so a failure rolls back all changes.

### Before you run the script

1. **Review the table pairs** declared near the top of the script. Each row has a `ProdQualified` (source) and `TestQualified` (target) table name. Edit that list if new tables have been duplicated or if a table should be skipped. The default list currently covers:
   - `Intraday.model.NettedWeight`
   - `Intraday.model.Model`
   - `Intraday.core.Security`
   - `Intraday.mkt.PriceBar`
   - `Intraday.mkt.FlatBar`
   - `Intraday.mkt.Stage_HistClose`
   - `Intraday.dbo.mkt_FlatBar_Staging`
   - `Intraday.wakett.Fill`
   - `Intraday.wakett.TradingLimit`
   - `Intraday.wakett.TradingLimitBreachReport`
   - `Intraday.wakett.Order`
2. **Confirm you have the required permissions.** You need rights to truncate and insert into the test tables and to read from the production tables. The script will create missing schemas/tables by issuing `CREATE SCHEMA` and `SELECT INTO`, so the account must also be able to create objects in the target database. If foreign keys reference a target table, disable those constraints temporarily or swap `TRUNCATE TABLE` for `DELETE FROM` inside the script.
3. **Back up the target tables** if you want to keep their current contents. The script truncates them before copying.

### Option A – SQL Server Management Studio (SSMS)

1. Open SSMS and connect to the SQL Server that hosts the intraday database.
2. Open a new query window and paste the contents of `RefreshTestTables.sql`.
3. Execute the script. Progress messages such as `Refreshing Intraday.model.NettedWeight -> Intraday.model_test.NettedWeight...` appear in the Messages tab. When the script finishes, `Command(s) completed successfully.` confirms the refresh.

### Option B – `sqlcmd`

Run the script directly from a terminal using the `sqlcmd` utility:

```bash
sqlcmd \
  -S <server_name> \
  -d master \
  -U <user_name> \
  -P '<password>' \
  -i scripts/sql/RefreshTestTables.sql
```

Replace `<server_name>`, `<user_name>`, and `<password>` with your credentials. Because every table name in the script is fully qualified, the initial database (`-d`) can be any database on the server.

### After the refresh

* Re-enable any foreign key or trigger that was disabled to allow the truncate operation.
* Spot-check a few tables—for example, compare row counts between the production and test tables to confirm they match.
* Check the output printed during execution for any errors. The transaction automatically rolls back if a copy fails.
