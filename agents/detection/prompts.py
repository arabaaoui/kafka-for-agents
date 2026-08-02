"""
Detection agent prompts — French supply chain anomaly detection.
"""

# System prompt that defines the detection agent's role and behavior.
SYSTEM_PROMPT = (
    "Tu es un agent de détection pour la supply chain d'un distributeur. "
    "Tu surveilles les topics Kafka 'stocks' et 'orders'. "
    "Détecte les anomalies : rupture de stock imminente, retard fournisseur, pic anormal."
)

# Template for anomaly detection from stock messages.
ANOMALY_DETECTION_PROMPT = """Analyse le message de stock suivant et détermine s'il s'agit d'une anomalie nécessitant une intervention.

Message stock reçu :
- store_id: {store_id}
- product_id: {product_id}
- quantity: {quantity}
- seuil_min: {seuil_min}
- region: {region}
- timestamp: {timestamp}

Critères d'anomalie :
1. Rupture de stock imminente : quantity < seuil_min
2. Stock critique : quantity < seuil_min * 0.5
3. Stock zéro : quantity == 0

Réponds au format JSON avec les champs suivants :
{{
    "anomalie": true/false,
    "type_anomalie": "RUPTURE_IMMINENTE" | "STOCK_CRITIQUE" | "STOCK_ZERO" | null,
    "severite": "HAUTE" | "CRITIQUE" | null,
    "message": "Description de l'anomalie détectée",
    "action_recommandee": "TRANSFERT_INTERNE" | "COMMANDE_FOURNISSEUR" | null
}}
"""