# Procédure de Réapprovisionnement Supply Chain

**Compétence :** Réapprovisionnement en cas d'anomalie de stock
**Agent :** Agent Décision (decision-agent)
**Version :** 1.0

---

## Contexte

Cette procédure est déclenchée par l'agent de décision lorsqu'une anomalie de stock est détectée (rupture imminente, stock sous le seuil minimum). L'objectif est de rétablir le niveau de stock du magasin concerné dans les meilleurs délais, en privilégiant les transferts internes entre magasins avant de passer commande auprès d'un fournisseur externe.

---

## Procédure en 6 étapes

### Étape 1 — Identification du produit et du magasin concernés

À partir du message d'anomalie reçu sur le topic `anomalies`, extraire les informations suivantes :

- **`store_id`** : identifiant du magasin en rupture ou sous le seuil minimum
- **`product_id`** : identifiant du produit concerné
- **`region`** : région du magasin (ex: `IDF`, `PACA`, `NORD`)
- **`current_quantity`** : quantité actuelle en stock
- **`seuil_min`** : seuil minimum de stock pour ce produit dans ce magasin

Calculer la **quantité nécessaire** : `quantite_necessaire = seuil_min - current_quantity + marge_securite`, où `marge_securite` correspond à 20% du seuil minimum.

---

### Étape 2 — Vérification des stocks des magasins voisins

Consulter le topic `stocks` pour identifier les magasins de la **même région** que le magasin en anomalie.

Pour chaque magasin voisin (hors magasin source), vérifier :

- **`stock_disponible`** : quantité actuelle pour le même `product_id`
- **`surplus`** : `stock_disponible - seuil_min` (le stock excédentaire après satisfaction de son propre seuil)

Ne retenir que les magasins dont le `surplus` est **supérieur ou égal à la quantité nécessaire**.

Classer les magasins voisins éligibles par `surplus` décroissant (le plus de surplus en premier).

---

### Étape 3 — Transfert interne (si un magasin voisin peut aider)

Si au moins un magasin voisin éligible a été identifié à l'étape 2 :

1. **Sélectionner** le magasin ayant le plus grand surplus.
2. **Déterminer la quantité à transférer** : `min(surplus_voisin, quantite_necessaire)`.
3. **Générer une tâche de transfert interne** sur le topic `tasks` avec les champs :
   - `action` : `"transfert_interne"`
   - `magasin_source` : le magasin voisin (celui qui donne)
   - `magasin_destination` : le magasin en anomalie (celui qui reçoit)
   - `product_id` : l'identifiant produit
   - `quantite` : la quantité à transférer
   - `priorite` : `"HAUTE"`
   - `region` : la région concernée
4. **Passer directement à l'étape 5** (sauter l'étape 4).

---

### Étape 4 — Commande fournisseur (si aucun magasin voisin ne peut aider)

Si **aucun magasin voisin** de la même région ne dispose du surplus nécessaire :

1. **Générer une tâche de commande fournisseur** sur le topic `tasks` avec les champs :
   - `action` : `"commande_fournisseur"`
   - `magasin_destination` : le magasin en anomalie
   - `product_id` : l'identifiant produit
   - `quantite` : la quantité nécessaire (après ajustement périssable si applicable)
   - `priorite` : `"CRITIQUE"`
   - `region` : la région concernée
2. **Estimer le délai de livraison** : 48h pour les produits secs, 24h pour les produits frais, 12h pour les produits ultra-frais. Inclure cette estimation dans la tâche (`delai_estime_heures`).

---

### Étape 5 — Ajustement pour les produits périssables

Si le produit est de type **périssable** (catégories : `FRAIS`, `ULTRA_FRAIS`, `SURGELE`) :

1. **Ajouter un buffer de 10%** pour compenser la perte et le gaspillage :
   - `quantite_ajustee = quantite * 1.10`
   - Arrondir à l'entier supérieur.
2. Mettre à jour le champ `quantite` dans la tâche avec cette valeur ajustée.
3. Ajouter un champ `perissable` : `true` et `buffer_gaspillage_pct` : `10` dans la tâche.

---

### Étape 6 — Journalisation de la décision dans le topic d'audit

Pour chaque décision prise (transfert ou commande), publier un message sur le topic `audit` avec les champs suivants :

- `timestamp` : horodatage ISO 8601 de la décision
- `anomalie_id` : identifiant de l'anomalie ayant déclenché la procédure
- `type_decision` : `"TRANSFERT_INTERNE"` ou `"COMMANDE_FOURNISSEUR"`
- `product_id` : identifiant du produit
- `store_id` : identifiant du magasin en anomalie
- `region` : région concernée
- `quantite_commandee` : quantité finale (après ajustement périssable si applicable)
- `magasin_source` : identifiant du magasin source (pour les transferts uniquement)
- `delai_estime_heures` : délai estimé (pour les commandes fournisseur uniquement)
- `perissable` : booléen indiquant si le produit est périssable
- `raison` : justification de la décision (ex: "Transfert depuis magasin X car surplus de Y unités" ou "Aucun magasin voisin avec surplus suffisant dans la région Z")

---

## Règles métier complémentaires

- **Priorité des transferts** : toujours privilégier un transfert interne (moins coûteux, plus rapide) avant une commande fournisseur.
- **Périmètre régional** : les transferts ne s'effectuent qu'entre magasins d'une même région (cohérence logistique).
- **Seuil de déclenchement** : la procédure se déclenche dès que `current_quantity < seuil_min`.
- **Traçabilité** : toute décision doit être journalisée dans le topic `audit` pour conformité et analyse ultérieure.