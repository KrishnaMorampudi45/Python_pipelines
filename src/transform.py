import re
from database import ssms
import pyodbc

def transform(path):
    with open(path,'r',encoding='utf-8') as f:
        name=f.readline()
        print(name)
        text=f.read()
        phone=re.search(r'(\+91[-\s]?|0)?[6789]\d{9}',text).group()
        print(phone)
        mail=re.search(r'[a-z0-9._%+-]+@[a-z]+.[a-z]{2,}',text).group()
        print(mail)
        skills_list = [
        # 🖥️ Programming Languages
        'python', 'java', 'c', 'c++', 'c#', 'javascript', 'typescript',
        'ruby', 'php', 'swift', 'go', 'rust', 'kotlin', 
        'bash', 'shell scripting', 'matlab', 'objective-c', 'perl',

        # 🧱 Frameworks & Libraries
        'django', 'flask', 'fastapi', 'spring boot', 'express.js',
        'react', 'angular', 'vue.js', 'next.js', 'nestjs',
        'jquery', 'bootstrap', 'tailwind css', 'dotnet', 'laravel',
        'tensorflow', 'keras', 'pytorch', 'scikit-learn', 'opencv',

        # 🗄️ Databases
        'mysql', 'postgresql', 'sqlite', 'mongodb', 'oracle',
        'sql server', 'redis', 'cassandra', 'couchbase',
        'elasticsearch', 'dynamodb', 'firebase realtime db',

        # ☁️ Cloud Platforms
        'aws', 'azure', 'google cloud platform', 'gcp',
        'heroku', 'digitalocean', 'netlify', 'vercel',

        # 🛠️ DevOps & Tools
        'docker', 'kubernetes', 'jenkins', 'terraform', 'ansible',
        'git', 'github', 'gitlab', 'bitbucket', 'circleci',
        'travis ci', 'vagrant', 'helm',

        # 📊 Data & Analytics
        'pandas', 'numpy', 'matplotlib', 'seaborn', 'power bi',
        'tableau', 'excel', 'hadoop', 'spark', 'airflow',
        'apache kafka', 'databricks', 'bigquery',

        # 🔒 Security & Networking
        'linux', 'windows server', 'firewall', 'ssl', 'tcp/ip',
        'ethical hacking', 'penetration testing', 'nmap',
        'wireshark', 'burpsuite', 'cybersecurity', 'oauth',

        # 🌐 Web & API Tech
        'html', 'css', 'sass', 'graphql', 'rest api', 'soap',
        'postman', 'swagger', 'json', 'xml',

    

        # 🎨 Design Tools
        'figma', 'adobe xd', 'photoshop', 'illustrator',
        'canva', 'blender', 'invision'
        ]
        low_text=text.lower()
        skills=[]
        for skill in skills_list:
            pattern = r'(?<!\w)' + re.escape(skill) + r'(?!\w)'
            if re.search(pattern, low_text):
                skills.append(skill)
            

        skills=','.join(skills)
        print(skills)

        con=ssms()
        cursor=con.cursor()
        cursor.execute("insert into resume_data(name,phone,email,skills) values (?,?,?,?)",name,phone,mail,skills)
        con.commit()
        