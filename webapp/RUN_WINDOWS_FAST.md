# Running DustReports on Windows (as fast as possible)

## 1. Install and run the ODBC driver

The app is faster when it uses **ODBC** to load data. You need an InterBase ODBC driver installed and registered on Windows.

### Option A: You already have Devart ODBC driver (paid)

If you used ODBC before and had “Devart ODBC Driver for InterBase” installed:

1. **Check the driver is there**
   - Press **Win + R**, type `odbcad32.exe`, Enter.
   - Open the **Drivers** tab.
   - Find **“Devart ODBC Driver for InterBase”** (or similar) in the list.

2. **Run the app** (see “Run the app” below).  
   If the driver name is different, set it before running:
   ```bat
   set IB_ODBC_DRIVER=Devart ODBC Driver for InterBase
   python app.py
   ```

No extra install steps needed.

---

### Option B: Free Embarcadero InterBase ODBC driver

**B1 – If you have InterBase 2020 installed**

- The ODBC driver is often included. Run the InterBase 2020 installer and ensure the **ODBC driver** component is selected, or look in the InterBase install folder for an ODBC setup/installer.
- After install, open **ODBC Data Source Administrator** (see “Check driver name” below) and confirm the driver appears on the **Drivers** tab.

**B2 – Build the free driver from source**

1. **Prerequisites**
   - **Visual Studio** (e.g. 2013 or later with C++ desktop workload), or **Visual Studio Build Tools**.
   - **Git** (optional, or download the repo as ZIP).

2. **Get the source**
   - Clone: `git clone https://github.com/Embarcadero/InterBaseODBCDriver.git`  
   - Or download ZIP from: https://github.com/Embarcadero/InterBaseODBCDriver and extract.

3. **Build (64-bit)**
   - Open a **Developer Command Prompt for VS** (or “x64 Native Tools Command Prompt”).
   - Go to the driver folder:
     ```bat
     cd path\to\InterBaseODBCDriver\Builds\MsVc120.win
     ```
   - Run:
     ```bat
     odbcbuild.bat
     ```
   - Or open the Visual Studio solution in that folder and build **Release x64**. The build produces a `.dll` (e.g. in a `Release` or `x64` output folder).

4. **Register the driver in Windows**
   - Press **Win + R**, type `odbcad32.exe`, Enter (use **64-bit** ODBC Administrator if your Python is 64-bit).
   - Go to the **Drivers** tab → **Add**.
   - If “Add” only lets you pick existing drivers, the driver must be registered in the system. Copy the built `.dll` to a permanent folder (e.g. `C:\Program Files\InterBase ODBC\`) and add a driver entry via **Add** by pointing to that `.dll` and setting the driver name (e.g. `InterBase ODBC Driver`).  
   - Exact steps depend on the driver’s install kit; if the repo includes an **Install** folder or setup, run that and then check the **Drivers** tab again.

5. **Check driver name**
   - In ODBC Data Source Administrator → **Drivers** tab, note the **exact** name (e.g. `InterBase ODBC Driver`). You will use this in the app (see “Driver name” below).

---

### Check driver name (any driver)

1. Press **Win + R**, type `odbcad32.exe`, Enter.
2. Open the **Drivers** tab.
3. Find the InterBase/Devart driver and copy its **exact** name (e.g. `InterBase ODBC Driver` or `Devart ODBC Driver for InterBase`).
4. If you use a name that’s not the default in the app, set it when running:
   ```bat
   set IB_ODBC_DRIVER=Exact Driver Name Here
   python app.py
   ```

### Disable ODBC (use direct InterBase only)

If you don’t want to use ODBC and only use the direct InterBase connection:

```bat
set USE_ODBC=0
python app.py
```

---

## 2. Install Python dependencies

From the `webapp` folder:

```bat
cd c:\projects\DustReports\webapp
pip install -r requirements.txt
```

This installs Flask, pyodbc, waitress, interbase, etc.

---

## 3. Run the app (after driver + pip install)

With the ODBC driver installed and dependencies installed, start the app:

### Production (recommended, faster)

Uses **Waitress** with 8 threads so multiple users get better throughput:

```bat
cd c:\projects\DustReports\webapp
python app.py
```

By default the app runs with Waitress. Open: **http://localhost:5000**

### Development (Flask built-in, single-threaded)

```bat
set USE_WAITRESS=0
python app.py
```

### Optional env vars

| Variable          | Default                    | Purpose                          |
|-------------------|----------------------------|----------------------------------|
| `USE_ODBC`        | `1`                        | Use ODBC when available (faster) |
| `IB_ODBC_DRIVER`  | `InterBase ODBC Driver`    | ODBC driver name                 |
| `USE_WAITRESS`    | `1`                        | Use Waitress server              |
| `FLASK_HOST`      | `0.0.0.0`                  | Bind address                     |
| `FLASK_PORT`      | `5000`                     | Port                             |
| `IB_DATA_SOURCE`  | (from config)              | InterBase server IP/host         |
| `IB_DATABASE_PATH`| (from config)              | Database path on server          |
| `IB_USERNAME`     | (from config)              | DB user                          |
| `IB_PASSWORD`     | (from config)               | DB password                      |

---

## 4. Summary for max speed

1. **Install and register** an InterBase ODBC driver (Embarcadero free or Devart).
2. **Leave `USE_ODBC=1`** (default) so the app uses ODBC for loading data.
3. **Leave `USE_WAITRESS=1`** (default) so the app runs with Waitress and 8 threads.
4. Run: `python app.py` from the `webapp` folder.

Data load will use ODBC (fast bulk fetch), and the app will serve concurrent requests via Waitress.
