import json
from datetime import datetime
from fastapi import FastAPI, HTTPException, BackgroundTasks,Request
from pydantic import BaseModel
import psycopg2
#import Clickup
import requests

# Configuración de PostgreSQL
DB_HOST = "db.g97.io"
DB_PORT = "5432"
DB_USER = "developer"
DB_PASSWORD = "qS*7Pjs3v0kw"
DB_NAME = "data_analyst"

# Configuración de ClickUp
CLICKUP_API_KEY = "pk_3182376_Q233NZDZ8AVULEGGCHLKG2HFXWD6MJLC"
CLICKUP_SPACE_ID = "900200532843"
CLICKUP_LIST_ID = "900200532843"

app = FastAPI()

# Configuración de HubSpot
HUBSPOT_API_KEY = "pat-na1-bfa3f0c0-426b-4f0e-b514-89b20832c96a"
HUBSPOT_CONTACTS_API = "https://api.hubapi.com/crm/v3/objects/contacts"



# Modelo para crear un contacto en HubSpot
class ContactHubSpot(BaseModel):
    email: str
    firstname: str
    lastname: str
    phone: str
    website: str

# Conexión a PostgreSQL
def create_postgres_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
        )

        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Evento startup de FastAPI
#@app.on_event("startup")
# async def startup_event():
#     try:
#         # Intenta establecer la conexión a PostgreSQL al inicio de la aplicación
#         create_postgres_connection()
#         print("Connected to PostgreSQL.")
#     except Exception as e:
#         print(f"Error connecting to PostgreSQL: {str(e)}")
#         raise HTTPException(status_code=500, detail="Error connecting to the database.")

# Ruta para crear un contacto en HubSpot y sincronizar con ClickUp
@app.post("/create_contact/")
async def create_contact(contact: ContactHubSpot):
    # Guardar en PostgreSQL
    conn = create_postgres_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO contact (firstname, lastname, email, phone, website) VALUES (%s, %s, %s,%s, %s)",
            (contact.firstname, contact.lastname, contact.email, contact.phone, contact.website),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))
    cur.close()
    conn.close()

    # Crear contacto en HubSpot
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json",
    }
    data = {
        "properties": {
            "firstname": contact.firstname,
            "lastname": contact.lastname,
            "email": contact.email,
            "phone": contact.phone,
            "website": contact.website
        }
    }
    response = requests.post(HUBSPOT_CONTACTS_API, headers=headers, json=data)
    response_data = response.json()
    if response.status_code != 201:
        raise HTTPException(status_code=500, detail=response_data)

    created_at = datetime.now()
    conn = psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()

    # Obtener el endpoint, el status de la solicitud y el tipo de petición
    endpoint = "sync_contacts/"

    result = response.status_code
    params = json.dumps({'method': 'post'})

    # Insertar el registro en la tabla de logs
    cur.execute(
        "INSERT INTO api_calls (endpoint, created_at, result, params) VALUES (%s, %s, %s, %s)",
        (endpoint, created_at, result, params),
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"este es el id", response_data["id"]}


# Función para obtener los contactos en HubSpot con estado_clickup=False
def get_hubspot_contacts_without_clickup_sync():
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json",
    }
    params = {
        "limit": 10,  # Puedes ajustar el número de contactos a obtener
        "properties": "firstname,lastname,email,estado_clickup",  # Propiedades a obtener para cada contacto
        "filter": "estado_clickup:false",  # Filtrar contactos con estado_clickup=False
    }

    response = requests.get(HUBSPOT_CONTACTS_API, headers=headers, params=params)
    response_data = response.json()
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=response_data)
    return response_data.get("results", [])

# Función para sincronizar un contacto a ClickUp como tarea en una lista
def sync_contact_to_clickup(contact_data):

    firstname = contact_data.get("firstname", "")
    lastname = contact_data.get("lastname", "")
    email = contact_data.get("email", "")

    task_data = {
        "name": f"{firstname} {lastname}",
        "content": f"Email: {email}",
    }

    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    headers = {
        "Authorization": CLICKUP_API_KEY,
    }

    response = requests.post(url, headers=headers, json=task_data)
    # Verificar si la solicitud fue exitosa (código de estado 200) y si la respuesta contiene datos JSON válidos
    if response.status_code == 200:
        try:
            response_data = response.json()
            if "id" in response_data:
                # La respuesta contiene datos JSON válidos y el ID de la tarea creada en ClickUp
                task_id = response_data["id"]
                print(f"Tarea creada en ClickUp con ID: {task_id}")
            else:
                print("Error: La respuesta de ClickUp no contiene el ID de la tarea.")
        except ValueError:
            print("Error: La respuesta de ClickUp no es un JSON válido.")
    else:
        print("Error: La solicitud a la API de ClickUp no fue exitosa.")

    created_at = datetime.now()
    conn = psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()

    # Obtener el endpoint, el status de la solicitud y el tipo de petición
    endpoint = "sync_contacts/"

    result = response.status_code
    params = json.dumps({'method': 'post'})


    # Insertar el registro en la tabla de logs
    cur.execute(
        "INSERT INTO api_calls (endpoint, created_at, result, params) VALUES (%s, %s, %s, %s)",
        (endpoint, created_at, result, params),
    )

    conn.commit()
    cur.close()
    conn.close()


# Ruta para sincronizar los contactos entre HubSpot y ClickUp
@app.post("/sync_contacts/")
async def sync_contacts(background_tasks: BackgroundTasks):
    try:
        hubspot_contacts = get_hubspot_contacts_without_clickup_sync()
        for contact in hubspot_contacts:
            background_tasks.add_task(sync_contact_to_clickup, contact)
        return {"message": "Sincronización iniciada en segundo plano."}
    except HTTPException as e:
        raise e

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

