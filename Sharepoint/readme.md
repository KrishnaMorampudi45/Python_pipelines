

---

# 📊 SharePoint to Database Data Pipeline

This script connects to a **SharePoint list**, retrieves data from it, and saves it into a **SQL database** using Python. It is designed to make SharePoint project data available in a structured database format for reporting, analysis, or integration purposes.

---

## 📁 What This Script Does

1. **Connects to a SharePoint site** using secure login credentials.
2. **Retrieves project data** from a specific SharePoint list called `Project_details`.
3. **Formats the data** into a table using pandas.
4. **Saves the data** into a database table named `Sharepoint_data`.

---

## 🔧 Files Used

* `config.ini`: Stores SharePoint login credentials (site URL, username, password).
* `database.py`: Contains a function called `conn()` that connects to your database.
* `main script`: The script you're running (the one shown above).

---

## 🧰 Python Packages Explained

Here are the tools (libraries) used and what they do:

### ✅ `office365.sharepoint.client_context`

* **What it does**: Connects to your SharePoint site and allows you to interact with lists and documents.
* **Used for**: Logging in and accessing the `Project_details` list.

### ✅ `office365.runtime.auth.user_credential`

* **What it does**: Lets you log in using your SharePoint username and password.
* **Used for**: Secure login to SharePoint.

### ✅ `configparser`

* **What it does**: Reads configuration files (like `config.ini`).
* **Used for**: Getting your SharePoint login credentials safely, without hardcoding them in your script.

### ✅ `pandas`

* **What it does**: A powerful data tool for organizing and transforming data into table-like structures.
* **Used for**: Creating a DataFrame (table) from SharePoint data, and saving it into the database.

### ✅ `from database import conn`

* **What it does**: Imports the `conn()` function from your `database.py` file.
* **Used for**: Connecting to your database so you can save the data there.

---

## 🧾 Script Breakdown

### Step 1: Read Login Info from Config File

```python
config.read('config.ini')
site_url = config['credentials']['your_site']
username = config['credentials']['username']
password = config['credentials']['password']
```

* This securely loads your SharePoint credentials.

---

### Step 2: Connect to SharePoint

```python
ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))
web = ctx.web
```

* Logs in and sets up a connection to your SharePoint site.

---

### Step 3: Access the SharePoint List

```python
items_ = web.lists.get_by_title('Project_details')
```

* Finds the list called **Project\_details** on your SharePoint site.

---

### Step 4: Select and Download Data

```python
items = items_.items.select([...]).get().execute_query()
```

* Pulls selected columns (like title, status, budget, etc.) from the list.

---

### Step 5: Format the Data

```python
for i in items:
    df.append({...})
df = pd.DataFrame(df)
```

* Converts the SharePoint data into a table (called a DataFrame).

---

### Step 6: Save Data to Database

```python
conn = conn()
df.to_sql('Sharepoint_data', conn, if_exists='replace', index=False)
```

* Saves the table to your database under the name `Sharepoint_data`.

---

## 🛡 Security Note

* Never share your `config.ini` file.
* It contains sensitive login credentials. Always add it to `.gitignore` if using version control.

---

## 🧪 Sample `config.ini` File

```ini
[credentials]
your_site = https://yourcompany.sharepoint.com/sites/YourSiteName
username = your.email@yourcompany.com
password = yourSecurePassword123
```

---

## ❓ FAQ

**Q: What is SharePoint?**
A: SharePoint is a web-based platform used by many companies to store and manage documents and data.

**Q: What is a SharePoint list?**
A: Think of it like an Excel spreadsheet stored on SharePoint. This script pulls rows from such a list.

**Q: What is a DataFrame?**
A: It's a table-like structure used in Python for organizing data.

---

