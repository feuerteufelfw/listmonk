import pandas as pd
import uuid
import json
from datetime import datetime, timezone

# Pfad zur alten CSV-Datei (mit Semikolon als Trennzeichen)
old_csv = 'listeanonym.csv'
# Pfad zur Ausgabedatei für Listmonk
new_csv = 'listmonk_import.csv'

# Einlesen der alten Datei
old_df = pd.read_csv(old_csv, sep=';', dtype=str)

records = []
for _, row in old_df.iterrows():
    # UUID für Listmonk
    subscriber_uuid = str(uuid.uuid4())

    # E-Mail und Name
    email = row.get('u_email', '').strip()
    vorname = row.get('u_vorname', '').strip()
    name = row.get('u_name', '').strip()
    full_name = f"{vorname} {name}".strip()

    # Erstellungsdatum und Aktualisierungsdatum aus Bestätigungsstempel
    stamp = row.get('u_email_bestaetigt_stamp', '')
    try:
        dt = datetime.strptime(stamp, '%d.%m.%Y %H:%M')
        # als UTC kennzeichnen
        dt = dt.replace(tzinfo=timezone.utc)
    except Exception:
        dt = datetime.now(timezone.utc)
    ts_str = dt.strftime('%Y-%m-%d %H:%M:%S') + ' +0000 UTC'

    # Attribute zusammenstellen (anpassen je nach Listmonk-Konfiguration)
    attrs = {}
    if 'u_ort' in row:
        attrs['city'] = row['u_ort']
    if 'u_plz' in row:
        attrs['zip'] = row['u_plz']
    # Beispiel für Newsletter-Flag (0/1)
    if 'u_starboxx_newsletter' in row:
        val = row['u_starboxx_newsletter']
        # in Boolean umwandeln
        attrs['newsletter'] = True if val in ['1', 'True', 'true'] else False
    if 'feedbacklink' in row:
        attrs['feedbacklink'] = row['feedbacklink']
    if 'unsubscribelink' in row:
        attrs['unsubscribelink'] = row['unsubscribelink']
    if 'fotouploadlink' in row:
        attrs['fotouploadlink'] = row['fotouploadlink']
    if 'medienuploadlink' in row:
        attrs['medienuploadlink'] = row['medienuploadlink']
    if 'u_anrede' in row:
        attrs['u_anrede'] = row['u_anrede']
    if 'projektname' in row:
        attrs['projektname'] = row['projektname']
    if 'u_anrede' in row:
        attrs['u_anrede'] = row['u_anrede']
    # Weitere Attribute hier hinzufügen

    # Tags (als Liste) – hier leer oder nach Bedarf füllen
    tags = []

    # Datensatz für Listmonk
    rec = {
        'uuid': subscriber_uuid,
        'email': email,
        'name': full_name,
        'attributes': json.dumps(attrs, ensure_ascii=False),
        'tags': json.dumps(tags, ensure_ascii=False),
        'status': 'enabled',
        'created_at': ts_str,
        'updated_at': ts_str
    }
    records.append(rec)

# Neues DataFrame im Listmonk-Format
new_df = pd.DataFrame(records, columns=[
    'uuid', 'email', 'name', 'attributes', 'tags',
    'status', 'created_at', 'updated_at'
])

# Als CSV speichern
new_df.to_csv(new_csv, index=False, encoding='utf-8')

print(f"Konvertierung abgeschlossen: {len(new_df)} Datensätze in '{new_csv}' geschrieben.")
