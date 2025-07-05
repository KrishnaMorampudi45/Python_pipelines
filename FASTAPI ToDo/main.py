from fastapi import FastAPI,HTTPException
from database import sql_conn
from pydantic import BaseModel
import pandas as pd

app=FastAPI()

class Todo(BaseModel):
    title:str
    description:str
    status: str

@app.get('/')
def home():
    return 'welcome to ToDo application'

#for new todo insertion
@app.post('/new_todo/')
def insert(new:Todo):
    conn = sql_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor=conn.cursor()
    cursor.execute("INSERT INTO todo (title, description, status) VALUES (%s, %s, %s)",(new.title,new.description,new.status))
    conn.commit()
    conn.close()
    return 'New Todo Inserted'

# Get todos by title
@app.get("/todos_by_title/{title}")
def todo_by_title(title:str):
    conn = sql_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor=conn.cursor()
    cursor.execute(f"select * from todo where title=%s",(title,))
    data=cursor.fetchall()
    conn.close()
    return data

# Get all todos
@app.get("/todos/")
def todo_by_title():
    conn = sql_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor=conn.cursor()
    cursor.execute(f"select * from todo")
    data=cursor.fetchall()
    conn.close()
    return data

#Deleting todo by title
@app.delete("/delete_todos/{title}")
def delete_todo(title:str):
    conn = sql_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor=conn.cursor()
    cursor.execute(f"delete from todo where title=%s",(title,))
    conn.commit()
    conn.close()
    return f"deleted todos with title named {title}"

#Update values 
@app.put('/update/{key}:{old_value}:{new_value}')
def update_todo(key:str,old_value:str,new_value:str):
    conn = sql_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor=conn.cursor()
    cursor.execute(f"update todo set {key}=%s where title=%s",(new_value,old_value,))
    conn.commit()
    conn.close()
    return f"updated {key} to {new_value}"

#Updating the status of particular title
@app.put('/update_status/{title}:{status}')
def status(title: str,status:str):
    conn = sql_conn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor=conn.cursor()
    cursor.execute(f"update todo set status=%s where title=%s",(status,title))
    conn.commit()
    conn.close()
    return f"status of title {title} is updated to {status}"






