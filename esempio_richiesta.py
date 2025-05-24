import requests
import json

def get_cik_data_from_api(cik: str, base_url: str = "http://localhost:8000"):
    """
    Recupera i dati di un CIK dall'API e ricostruisce il dizionario
    
    Args:
        cik (str): Il CIK da cercare
        base_url (str): URL base dell'API (default: http://localhost:8000)
    
    Returns:
        dict: Dati strutturati del CIK
    """
    endpoint = f"{base_url}/cik={cik}"
    
    try:
        response = requests.get(endpoint)
        response.raise_for_status()  # Solleva eccezione per status code 4xx/5xx
        
        # Ricostruisci il dizionario dalla risposta JSON
        cik_data = response.json()
        return cik_data
    
    except requests.exceptions.HTTPError as e:
        print(f"Errore HTTP: {e}")
        if response.status_code == 404:
            print("CIK non trovato nel database")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Errore di connessione: {e}")
        return None

# Esempio di utilizzo
if __name__ == "__main__":
    cik_example = "0001872538"  # Sostituisci con un CIK valido
    result = get_cik_data_from_api(cik_example)
    
    if result:
        print(type(result))
        print(result.keys())
        print("Dati del CIK ottenuti con successo:")
        #t = json.dumps(result, indent=2)
        #print(t.keys())
    else:
        print("Impossibile ottenere i dati del CIK")
