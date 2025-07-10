Here's the full text you can directly paste into a `README.md` file:

---

```md
# 📄 Resume Parsing & Archiving System

This project automates the extraction, transformation, and archival of resume data (in PDF format) stored in an **Amazon S3 bucket**. It parses resumes to extract relevant information like name, phone, email, and skills, stores the results in a **SQL Server** database, and archives the processed files to another S3 bucket.

---

## 📁 Project Structure

```

.<br>
├── main.py               # Main script to extract and process PDF resumes<br>
├── transform.py          # Extracts structured data from resume text<br>
├── database.py           # Manages SQL Server connection<br>
├── to\_archive.py         # Moves processed resumes to an archive bucket<br>
├── config.ini            # Configuration file<br>
├── pdf\_text\_output/      # Folder for extracted .txt files from PDFs<br>
└── README.md             # Project documentation<br>

````

---

## 🧰 Requirements

- `boto3` – AWS SDK for Python (S3 interactions)
- `PyMuPDF (fitz)` – For reading and extracting text from PDF files
- `sqlalchemy` – SQL connection abstraction
- `pyodbc` – ODBC driver interface for SQL Server
- `re` – Python regular expressions for data extraction
- `os`, `io`, `configparser` – Standard Python libraries for file and configuration handling

Install the dependencies using:

```bash
pip install boto3 pymupdf sqlalchemy pyodbc
````

---

## ⚙️ Configuration File (`config.ini`)

Create a `config.ini` file in the root directory with the following content:

```ini
[s3]
bucket = your-source-bucket-name

[prefix]
path = your/prefix/path/

[ssms]
engine = your_db_string
```

---

## 🚀 How It Works

### 🧩 main.py

* Connects to the source S3 bucket
* Lists all PDF files under a specified prefix
* Downloads and extracts text using `fitz` (PyMuPDF)
* Saves the raw text as `.txt` in `pdf_text_output/`
* Calls `transform()` to extract name, phone, email, skills
* Calls `archive()` to move the processed PDF to an archive bucket

---

### 🔎 transform.py

* Opens the extracted `.txt` file
* Extracts:

  * **Name** – First line of the file
  * **Phone Number** – Using regex (supports Indian formats)
  * **Email** – Using regex
  * **Skills** – Matches against a predefined list of 100+ tech skills
* Saves the parsed data to SQL Server

**Skills Covered Include:**

* Programming (Python, Java, C++, etc.)
* Frameworks (Django, React, Spring Boot, etc.)
* Databases (MySQL, PostgreSQL, MongoDB, etc.)
* DevOps Tools (Docker, Kubernetes, Jenkins, etc.)
* Data Tools (Pandas, NumPy, Power BI, etc.)
* Security & Networking, APIs, Design Tools, and more.

```python
cursor.execute("INSERT INTO resume_data(name, phone, email, skills) VALUES (?, ?, ?, ?)", ...)
```

---

### 🗃️ database.py

Handles the SQL Server connection using `sqlalchemy`. Reads the connection string from `config.ini`.

```python
engine = create_engine(config['ssms']['engine'])
return engine.raw_connection()
```

---

### 📦 to\_archive.py

Handles S3 archival of processed resumes:

* Copies the processed PDF from the source bucket to the destination bucket (`kri4545bucky`) under `archives/`
* Deletes the original file from the source

```python
s3.meta.client.copy(copy_file, dest_bucket, dest_key)
s3.Object(source_bucket, source_file).delete()
```

---

## 💾 Output

* Text extracted from resumes is saved in `pdf_text_output/`
* Structured data is saved in the `resume_data` table in your SQL Server DB
* Original PDFs are moved to the `archives/` folder in another S3 bucket

---


---

## 🛡️ Security Tips

* Never hardcode AWS credentials or DB passwords
* Use `.env` files or secrets managers for production
* Ensure database and S3 access is controlled via IAM roles

---

## 🧪 Sample Table Schema (SQL Server)

```sql
CREATE TABLE resume_data (
    id INT IDENTITY PRIMARY KEY,
    name NVARCHAR(255),
    phone VARCHAR(20),
    email NVARCHAR(255),
    skills NVARCHAR(MAX),
    created_at DATETIME DEFAULT GETDATE()
);
```

---

## 👨‍💻 Author

Feel free to contribute or report issues via GitHub!

```

Let me know if you want this saved to a file or adjusted for another markdown flavor (like GitHub Pages, Jupyter, etc.).
```
