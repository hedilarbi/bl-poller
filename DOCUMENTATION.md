# BL-Poller — Documentation complète du projet

> Dernière mise à jour : 2026-05-26

---

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Architecture globale](#2-architecture-globale)
3. [Structure des fichiers](#3-structure-des-fichiers)
4. [Les deux plateformes surveillées](#4-les-deux-plateformes-surveillées)
5. [Cycle de polling — boucle principale](#5-cycle-de-polling--boucle-principale)
6. [Authentification P1 — auto-refresh et Playwright](#6-authentification-p1--auto-refresh-et-playwright)
7. [Authentification P2 — portal Athena](#7-authentification-p2--portal-athena)
8. [Logique de filtrage des offres](#8-logique-de-filtrage-des-offres)
9. [Cache des trajets acceptés (rides cache)](#9-cache-des-trajets-acceptés-rides-cache)
10. [Réservation automatique des offres](#10-réservation-automatique-des-offres)
11. [Notifications Telegram](#11-notifications-telegram)
12. [Couche de données (DB shim + EC2 API)](#12-couche-de-données-db-shim--ec2-api)
13. [Gestion de l'état en mémoire](#13-gestion-de-létat-en-mémoire)
14. [Coordination multi-utilisateur](#14-coordination-multi-utilisateur)
15. [Métriques de performance](#15-métriques-de-performance)
16. [Configuration — variables d'environnement](#16-configuration--variables-denvironnement)
17. [Démarrage et déploiement](#17-démarrage-et-déploiement)
18. [Flux de données complet (diagramme)](#18-flux-de-données-complet-diagramme)

---

## 1. Vue d'ensemble

**bl-poller** est un bot de surveillance automatique des offres de courses Blacklane pour chauffeurs.  
Il tourne en continu sur un serveur VPS, interroge les APIs Blacklane toutes les **300 ms**, filtre les offres selon les préférences du chauffeur, et **accepte automatiquement** celles qui correspondent — le tout avant que le chauffeur n'ait eu à lever le petit doigt.

### Ce que fait le bot, concrètement

1. **Polls les offres** sur deux APIs Blacklane (application mobile + portail partenaire).
2. **Filtre** chaque offre selon : classe de véhicule, prix, distance, horaires de travail, jours bloqués, listes noires d'adresses/vols, créneaux réservés, et conflits avec les courses déjà acceptées.
3. **Réserve automatiquement** (fire-and-forget) les offres qui passent les filtres.
4. **Notifie le chauffeur** via Telegram avec le résultat (acceptée, rejetée, raison).
5. **Gère les tokens expirés** : tente un refresh OAuth classique, puis un login PKCE headless via Playwright si nécessaire.

### Contexte de déploiement

- **Un VPS = un utilisateur** (chauffeur).  
  La variable `ASSIGNED_BOT_ID` + `ASSIGNED_TELEGRAM_ID` dans `.env` lie ce VPS à un compte précis.
- **EC2 = cerveau central** : stocke les configurations, tokens, logs d'offres et préférences. Le VPS n'a pas de base de données locale — tout passe par l'API interne EC2.
- **Telegram Bot** : chaque chauffeur reçoit ses notifications dans sa propre conversation avec son bot dédié.

---

## 2. Architecture globale

```
┌─────────────────────────────────────────────────────────┐
│                     VPS (bl-poller)                     │
│                                                         │
│  main.py ──► loop.py                                    │
│                │                                        │
│     ┌──────────┼──────────┐                             │
│     ▼          ▼          ▼                             │
│  p1_client  p2_client  p1_auth                          │
│  (mobile)   (portail)  (Playwright PKCE)                │
│     │          │                                        │
│     └────┬─────┘                                        │
│          ▼                                              │
│      processing.py                                      │
│    (filtrage + réservation)                             │
│          │                                              │
│    ┌─────┼─────┐                                        │
│    ▼     ▼     ▼                                        │
│ filters notify  db.py (shim local)                      │
│              │                                          │
│              ▼                                          │
│          ec2_api.py ──────────────► EC2 Backend         │
│                                    (config, logs,        │
│                                     tokens, etc.)       │
└─────────────────────────────────────────────────────────┘
         │                    │
         ▼                    ▼
  Blacklane P1 API     Blacklane P2 API
  (chauffeur-app)      (partner-portal-api)
```

---

## 3. Structure des fichiers

```
bl-poller/
├── main.py                        # Point d'entrée — lance run()
├── db.py                          # Shim DB (cache local + appels EC2)
├── requirements.txt               # Dépendances Python
├── .env.example                   # Modèle de configuration
└── poller_core/
    ├── __init__.py
    ├── config.py                  # Toute la configuration (env vars)
    ├── loop.py                    # Boucle principale de polling
    ├── p1_client.py               # Client API Plateforme 1 (mobile)
    ├── p2_client.py               # Client API Plateforme 2 (portail)
    ├── p1_auth.py                 # Auth P1 : refresh OAuth + PKCE Playwright
    ├── processing.py              # Filtrage + réservation + notifications
    ├── filters.py                 # Logique des filtres (custom et built-in)
    ├── ec2_api.py                 # Client HTTP vers EC2 interne
    ├── notify.py                  # Envoi de messages Telegram
    ├── state.py                   # État en mémoire (rides cache, tokens, etc.)
    ├── offer_coordinator.py       # Coordination inter-utilisateurs
    ├── rides.py                   # Extraction d'intervalles depuis /rides
    ├── metrics.py                 # Métriques de latence (p50/p95)
    ├── timeparse.py               # Parsing ISO 8601 robuste
    ├── utils.py                   # Fonctions utilitaires (formatage, parsing)
    └── reserve_async.py           # (Helpers de réservation asynchrone)
```

---

## 4. Les deux plateformes surveillées

### Plateforme 1 (P1) — Application mobile chauffeur

| Propriété | Valeur |
|---|---|
| Base URL | `https://chauffeur-app-api.blacklane.com` |
| Endpoint offres | `GET /api/v1/chauffeur/offers?order_by=ASC&page=1&page_size=100&sort_by=start_time` |
| Endpoint rides | `GET /rides` |
| Endpoint réservation | `POST /offers` |
| Authentification | JWT Bearer token issu de l'application mobile iOS |
| User-Agent simulé | `Chauffeur/24531 CFNetwork/3860.300.31 Darwin/25.2.0` |
| Fréquence poll | 300 ms (base) |

L'API P1 utilise le protocole de l'application iOS officielle. Les headers `X-Request-ID`, `X-Correlation-ID`, et les headers de tracing Datadog (RUM) sont générés à chaque requête pour imiter le comportement réel de l'app.

### Plateforme 2 (P2) — Portail partenaire (Athena)

| Propriété | Valeur |
|---|---|
| Endpoint offres | `https://partner-portal-api.blacklane.com/api/v1/chauffeur/offers` |
| Endpoint offres (legacy Athena) | `https://athena.blacklane.com/hades/offers` |
| Endpoint rides | `https://athena.blacklane.com/hades/rides` |
| Endpoint réservation | `POST /api/v1/chauffeur/offers/{id}/acceptance` |
| Authentification | OAuth2 implicit grant via `https://athena.blacklane.com/oauth/token` |
| User-Agent simulé | Chrome 146 sur macOS |
| Fréquence poll | Cycle burst/calm : 0.8 s (5 min) puis 2 s (1 min), en boucle |

P2 utilise le format JSON:API avec des objets `included` pour les localisations. Le bot décode ces relations pour extraire les adresses de pickup/dropoff.

---

## 5. Cycle de polling — boucle principale

Le fichier central est `poller_core/loop.py`. La fonction `run()` est le cœur du système.

### Déroulement d'un cycle (tous les ~300 ms)

```
run()
 │
 ├─ 1. Récupère la liste des utilisateurs actifs (cache 3s pour éviter une requête EC2 à chaque cycle)
 │
 ├─ 2. Pour chaque utilisateur → poll_user(user)
 │       │
 │       ├─ a. Charge la config utilisateur depuis le cache runtime
 │       │      (ou appel EC2 /config si cache_version a changé)
 │       │
 │       ├─ b. Vérifie/initialise le rides cache (conflits avec courses existantes)
 │       │
 │       ├─ c. Détermine si P1 est en cooldown (token invalide, Playwright en cours, etc.)
 │       │
 │       ├─ d. Lance P1 + P2 en parallèle via ThreadPoolExecutor
 │       │      P1 → get_offers_p1() → GET /api/v1/chauffeur/offers
 │       │      P2 → _partner_get_offers() → GET /api/v1/chauffeur/offers (portail)
 │       │
 │       ├─ e. Traite les offres P1 immédiatement dès leur arrivée
 │       │      (sans attendre la fin du fetch P2 — optimisation latence)
 │       │
 │       ├─ f. Traite les nouvelles offres P2 (présence tracking : seules les nouvelles)
 │       │
 │       └─ g. _process_offers_for_user() → filtrage + réservation
 │
 └─ 3. time.sleep(POLL_INTERVAL)  # 0.3 s par défaut
```

### Burst mode

Quand une offre est trouvée, le système entre en **burst mode** pour quelques secondes (configurable via `BURST_DURATION_S`) et accélère la fréquence de poll (`BURST_POLL_INTERVAL_S`). Ce mécanisme permet de capturer les offres qui apparaissent en rafale.

### P2 Burst/Calm cycle

Le polling P2 suit un cycle autonome basé sur `time.time() % (300 + 60)` :
- **Phase burst (300 s)** : poll toutes les 0.8 s — aggressif mais efficace
- **Phase calm (60 s)** : poll toutes les 2 s — économique, protège contre le rate-limit

---

## 6. Authentification P1 — auto-refresh et Playwright

Le fichier `poller_core/p1_auth.py` gère les 3 niveaux d'authentification P1.

### Niveau 1 — Token valide
Le JWT token de l'application mobile est utilisé directement. Sa validité est vérifiée en décodant le payload sans signature (champ `exp`).

### Niveau 2 — Refresh OAuth
Appelé par `_try_p1_oauth_refresh()` (dans `poll_user`), au plus une fois par minute : **avant** l'expiration du JWT (marge `P1_REFRESH_SKEW_S` = 90 s) et sur un 401/403 du poll, **avant** de tenter Playwright. Le nouveau token (préfixé `Bearer `) et le `refresh_token` renouvelé sont sauvegardés sur l'EC2. Un refresh qui changerait de compte chauffeur (`sub` / `chauffeur_id` différents) est refusé. Nécessite un `refresh_token` dans `mobile_auth` : il est enregistré après chaque login Playwright réussi, et n'est plus effacé quand l'utilisateur colle un nouveau token dans le bot.
```
refresh_p1_access_token()
  POST https://login-chauffeur.blacklane.com/oauth/token
  Body: { grant_type: "refresh_token", client_id, refresh_token }
  → Retourne un nouveau access_token + refresh_token
```
Les headers de la requête OAuth reproduisent ceux de l'app mobile (`auth0-client`, cookies, etc.).

### Niveau 3 — Playwright PKCE (login headless)
Si le refresh token est aussi expiré/invalide, le bot fait un **vrai login** via un navigateur headless (Playwright + Chromium) :

```
_playwright_pkce_login(email, password)
  1. Lance Chromium headless (mode mobile iPhone simulé)
  2. Navigue vers l'URL d'autorisation PKCE OAuth2
  3. Remplit le formulaire email/password (délais randomisés pour éviter la détection bot)
  4. Intercepte la redirection vers le deep-link iOS (com.blacklane.chauffeur://)
  5. Extrait le code d'autorisation
  6. Échange le code contre un access_token + refresh_token via POST /oauth/token
```

**Protections anti-détection** :
- Masquage du flag `navigator.webdriver`
- User-Agent iPhone iOS 18
- Délais de frappe aléatoires (300–800 ms)
- Verrou global `_playwright_lock` : un seul login Playwright à la fois
- Intervalle minimum entre deux tentatives : 10 minutes (`_AR_MIN_INTERVAL_S`)
- Après 3 échecs consécutifs (`_AUTO_REFRESH_FAIL_THRESHOLD`) : pause de 1 h (`_AR_COOLDOWN_S`), respectée aussi sur le chemin 401/403
- Si la page de login affiche une vérification humaine (captcha) : aucune nouvelle tentative Playwright pendant 6 h (`_AR_CAPTCHA_COOLDOWN_S`) et un message Telegram demande un nouveau token. Le refresh par `refresh_token` (niveau 2) continue d'être tenté, car il ne passe pas par la page de login.

---

## 7. Authentification P2 — portal Athena

Simple et directe :
```
_athena_login(email, password)
  POST https://athena.blacklane.com/oauth/token
  Body: { client_id, username, password, grant_type: "implicit", resource_owner_type: "driver" }
  → Retourne un access_token JWT
```

Le token P2 est :
- Stocké en mémoire dans `state.py` (`_portal_token_mem`) avec expiration automatique basée sur le champ `exp` du JWT (moins 5 min de marge de sécurité).
- Persisté sur EC2 via `ec2_api.save_portal_token()` pour survivre aux redémarrages.
- Renouvelé automatiquement si expiré avant chaque poll P2.

---

## 8. Logique de filtrage des offres

Le fichier `poller_core/processing.py` implémente le pipeline de filtrage dans `_process_offers_for_user()`.

Chaque offre passe par les filtres dans cet ordre exact (court-circuit dès qu'un filtre rejette) :

### Filtres built-in

| # | Nom | Description |
|---|-----|-------------|
| 0a | **Classe véhicule** | La combinaison `(type_ride, vehicleClass)` doit être activée dans `class_state`. C'est le filtre le moins coûteux — vérifié en premier. |
| 0b | **Horaires de travail** | L'heure de pickup doit être dans la plage `work_start–work_end` (fuseau horaire du chauffeur). |
| 0b | **Jours bloqués** | Le jour du pickup ne doit pas figurer dans `blocked_days` (format `dd/MM/yyyy`). |
| 1 | **Délai minimal (gap)** | Le pickup doit être dans au moins `gap` minutes à partir de maintenant. |
| 1.5 | **Durée horaire min/max** | Pour les rides `hourly`, filtre sur `durationMinutes`. Configuré en heures (`min_hourly_hours`, `max_hourly_hours`). |
| 2 | **Prix min/max** | `price_min` ≤ prix ≤ `price_max`. |
| 2.5 | **Distance min/max** | Pour les rides `transfer` : `min_km`–`max_km`. Pour les rides `hourly` : `min_hourly_km`–`max_hourly_km` sur les km inclus. |
| 3 | **Pickup blacklist** | L'adresse de pickup ne doit pas contenir les termes de la liste. Match mot entier (boundary regex) pour les tokens simples. |
| 3 | **Dropoff blacklist** | Idem pour le dropoff. |
| 3.5 | **Vols bloqués** | Le numéro de vol (normalisé sans caractères spéciaux) ne doit pas figurer dans `flight_blacklist`. |
| 5 | **Créneaux bloqués** | Le ride ne doit pas chevaucher un `booked_slot` utilisateur (plages horaires manuellement bloquées). |
| 5.5 | **Conflit avec courses acceptées** | Le ride ne doit pas chevaucher une course déjà acceptée (rides cache + offres en cours d'acceptation dans ce cycle). Si `end_dt` est inconnu, une fenêtre de sécurité de 4h est utilisée. |

### Filtres personnalisés (custom filters)

Définis dans `poller_core/filters.py`, activés par slug via EC2 :

| Slug | Description |
|------|-------------|
| `pickup_airport_reject` | Rejette toute offre avec « airport » ou « aéroport » dans l'adresse de pickup. |
| `block_baby_seat` | Rejette toute offre mentionnant « baby seat » dans les demandes spéciales (cherche récursivement dans tous les champs). |
| `reject_under_90_between_20_22` | Entre 20h et 22h, rejette les offres dont le prix est inférieur à 90 (paramétrable). |

Un filtre custom peut retourner `"accept"` pour **forcer l'acceptation** malgré des filtres built-in échoués (override). Dans ce cas, l'offre est marquée `forced_accept = True` dans le log et la notification.

### Verdict final

```
failed_filters = [f for f in filter_results if not f["ok"]]

is_rejected = bool(failed_filters) AND NOT accept_override

→ Rejeté  : log + notification "rejected" (optionnel)
→ Accepté : lancement de la réservation asynchrone
```

---

## 9. Cache des trajets acceptés (rides cache)

Pour éviter d'accepter deux courses qui se chevauchent, le bot maintient un **cache des intervalles occupés** en mémoire (`state.py`).

### Source : même endpoint que la page current-schedule
`_fetch_scheduled_rides()` (dans `processing.py`) récupère les courses programmées :
- **Si les identifiants BL (email/mot de passe) existent** : `GET https://athena.blacklane.com/hades/rides?filter[group]=planned` (toutes les pages). Le token portail est recréé par login si absent ou rejeté (401/403), donc il ne reste jamais expiré. On garde les courses attribuées au chauffeur (`assigned_driver == bl_uuid`) **et** celles pas encore attribuées ; celles d'un autre chauffeur sont ignorées.
- **Sinon** : `GET https://chauffeur-app-api.blacklane.com/rides` (token mobile P1), comme la page current-schedule.

La fin de chaque course est **recalculée avec les end-time formulas** de l'utilisateur (voir ci-dessous).

### Initialisation
Au premier cycle pour un utilisateur, `_init_rides_cache_async()` (un seul job à la fois) remplit le cache via `_fetch_scheduled_rides()`.

### Calcul de `end_dt`
Ordre de priorité pour déterminer la fin d'une course :
1. Ride **hourly** : `pickupTime + durationMinutes + 2h buffer` (sécurité post-course)
2. Ride **transfer** avec formule admin : `pickupTime + (distance / speed_kmh * 2) + bonus_min`
3. Champ `endsAt` / `ends_at` / `end_time` de l'API
4. `estimatedDurationMinutes` de l'API
5. Si aucun : `None` → fenêtre de sécurité de 4h dans `_find_conflict()`

### Rafraîchissement périodique
Le cache se rafraîchit toutes les `RIDES_REFRESH_INTERVAL_S` secondes (défaut : 60 s) via `_refresh_rides_cache_async()`. Le timestamp est touché **avant** le fetch pour éviter les rafraîchissements dupliqués dans des cycles parallèles. Si la source échoue (401/403, timeout, 429, 5xx), **le cache précédent est conservé** (il n'est jamais remplacé par une liste vide).

### Registre des courses réservées par le bot (source toujours disponible)
2ᵉ source de vérité, si l'API échoue ou ne renvoie rien (échecs, délai, course pas encore listée). Chaque offre dont la réservation est **envoyée** est donc inscrite dans un registre (`add_local_reserved()` dans `state.py`) avec son `(pickup, endsAt calculé par formule)` :
- consulté par le contrôle de conflit à chaque offre, en plus du cache `/rides` ;
- persisté dans `RESERVED_RIDES_FILE` (défaut : `cache/reserved_rides.json`) et rechargé au démarrage ;
- une course n'en sort que lorsqu'elle est **terminée**, ou si Blacklane **refuse explicitement** la réservation (4xx). En cas de timeout / 5xx (résultat inconnu), le créneau reste bloqué 30 min.

### Logs à vérifier sur le VPS
- `🚗 scheduled rides [athena] UPDATED: …` — au démarrage et dès qu'une course ou une heure de fin change : une ligne `✅` par course occupée (id, type, statut, chauffeur, début → fin en heure locale, **comment la fin a été calculée** : formule utilisée, distance, vitesse, bonus), une ligne `✖` par course ignorée (avec la raison), puis le contenu du registre `📌`.
- `🚗 scheduled rides [athena] unchanged: N busy` — refresh réussi sans changement.
- `⚠️ scheduled rides [athena] fetch failed (…) — keeping previous rides` — la source a échoué, l'ancien cache est conservé.
- `📌 registry + / − / kept …` — une offre entre dans le registre (réservation envoyée), en sort (refus explicite), ou reste bloquée 30 min (résultat inconnu).

### Ajout en temps réel
Après chaque réservation réussie, `add_ride_to_cache()` ajoute aussi l'intervalle au cache `/rides` (il sera remplacé au prochain refresh ; le registre ci-dessus, lui, le garde).

### Nettoyage
Une fois par jour (`RIDES_CLEANUP_INTERVAL_S` = 86400 s), les courses passées (terminées il y a plus de 2h) sont supprimées du cache.

---

## 10. Réservation automatique des offres

Quand une offre passe tous les filtres, la réservation est lancée en **fire-and-forget** (non-bloquant).

### Pipeline de réservation

```
_process_offers_for_user()
  │
  ├─ 1. Vérifie qu'aucune réservation n'est déjà en vol pour cette offre
  │      (_pending_reserves dict)
  │
  ├─ 2. Calcule predicted_end (pour mettre à jour le rides cache si réussite)
  │
  ├─ 3. Soumet _reserve_offer_sync() au _reserve_executor (pool 16 threads)
  │
  ├─ 4. Marque l'intervalle dans current_cycle_intervals
  │      (bloque les offres suivantes du MÊME cycle qui se chevaucheraient)
  │
  ├─ 5. Ajoute l'offre dans _pending_reserves (TTL 15s)
  │
  ├─ 6. Enregistre le candidat dans offer_coordinator (pour notifications pairs)
  │
  └─ 7. Attache _on_reserve_done comme callback du future
```

### Appels API de réservation

**P1 :**
```
POST https://chauffeur-app-api.blacklane.com/offers
Body: { "id": offer_id, "action": "accept", "price": float }
Headers: JWT Bearer + headers mobiles complets
```

**P2 :**
```
POST https://partner-portal-api.blacklane.com/api/v1/chauffeur/offers/{id}/acceptance
Body: { "currency": "USD", "price": float, "price_minor_unit": int, "type": "prebooked" }
Headers: Bearer Athena token + X-User-Id + X-User-Lsp-Id + X-User-Bd-Id
         (les claims lsp_id et bd_id viennent du token P1, pas P2)
```

### Gestion du résultat (`_on_reserve_done`)

| Status HTTP | Action |
|-------------|--------|
| 200–299 | ✅ Succès → `add_ride_to_cache()` + notification Telegram "Accepted" |
| 401 (P1) | Token expiré → `set_token_status("expired")` |
| 409 / 410 | Offre déjà prise/expirée → notification "Not Accepted" avec raison |
| 422 | Offre invalide → notification "Not Accepted" |
| Autres erreurs | Notification "Not Accepted" |

### Sessions HTTP partagées pour la réservation

P1 et P2 maintiennent chacun une **session HTTP partagée unique** (`_reserve_session`, `_p2_reserve_session`) — contrairement aux sessions de polling qui sont thread-local. Cela garde la connexion TCP/TLS chaude entre les réservations (rares mais critiques en termes de latence). Un warmup périodique toutes les ~45 s maintient la connexion active.

---

## 11. Notifications Telegram

Le module `poller_core/notify.py` gère toutes les communications Telegram.

### Types de notifications

| Kind | Quand | Contenu |
|------|-------|---------|
| `accepted` | Réservation HTTP 2xx | Prix, classe, type, pickup, dropoff, durée, distance, numéro de vol, demandes spéciales |
| `not_accepted` | Échec de réservation (409/410/etc.) | Même info + raison de l'échec |
| `rejected` | Filtres non respectés | Même info + liste des filtres échoués |

Chaque notification inclut un bouton **"Show details"** avec un `callback_data` pointant vers un `message_key` stocké sur EC2 — le chauffeur peut afficher les détails complets dans son bot Telegram.

### Préférences de notification
L'utilisateur peut désactiver certains types (`accepted`, `not_accepted`, `rejected`) via sa configuration EC2. Le paramètre `force_notify=True` contourne cette préférence pour les notifications critiques (offre prise par quelqu'un d'autre, par exemple).

### Avertissements épinglés (pinned warnings)
Quand le token P1 est manquant ou expiré, un message d'avertissement est **épinglé** dans la conversation Telegram. Il est automatiquement désépiglé quand le token redevient valide.

### Format des messages
Les messages utilisent le HTML Telegram (`<b>`, `<code>`, etc.). En cas d'erreur de parsing HTML, le bot retente automatiquement en texte brut (fallback).

### Fragmentation
Les textes trop longs sont automatiquement découpés en chunks de 4096 caractères (limite Telegram).

---

## 12. Couche de données (DB shim + EC2 API)

### Le shim `db.py`

`db.py` est une **couche d'abstraction** qui présente exactement la même interface que `bl-bot/db.py` (la base de données centrale) mais redirige :
- Les **lectures** vers un cache en mémoire local (peuplé depuis EC2)
- Les **écritures** vers l'API EC2 interne

Cela permet aux modules `poller_core/*` d'importer `from db import ...` sans modification, comme s'ils tournaient sur le serveur principal.

### Cache local du shim

```python
_config: dict          # (bot_id, telegram_id) → config dict complète
_portal_tokens: dict   # (bot_id, telegram_id) → portal token string
_pinned_warnings: dict # (bot_id, telegram_id) → {no_token_msg_id, expired_msg_id}
_offer_messages: dict  # (bot_id, telegram_id, message_key) → (header, full_text)
```

### `prime_config_cache()`

Appelé par `loop.py` lors d'un cache-miss (changement de `cache_version`). Peuple le shim en une seule requête EC2 au lieu d'une requête par getter.

### `ec2_api.py` — Endpoints EC2

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/internal/poller/users` | GET | Liste des utilisateurs actifs |
| `/internal/poller/user/{bot_id}/{tg_id}/config` | GET | Config complète d'un utilisateur |
| `/internal/poller/user/{bot_id}/{tg_id}/token-status` | POST | Met à jour le statut du token |
| `/internal/poller/user/{bot_id}/{tg_id}/token` | POST | Sauvegarde token + headers + auth_meta |
| `/internal/poller/user/{bot_id}/{tg_id}/auto-refresh` | POST | Active/désactive l'auto-refresh |
| `/internal/poller/user/{bot_id}/{tg_id}/portal-token` | POST | Sauvegarde le token portail |
| `/internal/poller/pinned-warning/save` | POST | Sauvegarde l'ID d'un message épinglé |
| `/internal/poller/pinned-warning/clear` | POST | Efface l'ID d'un message épinglé |
| `/internal/poller/offer-message` | POST | Sauvegarde le rendu d'une offre |
| `/internal/poller/offer-log` | POST | Log une décision d'offre (stats) |

Toutes les requêtes EC2 sont authentifiées avec un header `Authorization: poller {POLLER_API_KEY}`.

---

## 13. Gestion de l'état en mémoire

`poller_core/state.py` centralise tout l'état en mémoire du processus.

### Tokens invalides

```python
_invalid_token_users: Dict[(bot_id, tg_id), (token_fingerprint, cache_version)]
```
Quand P1 retourne 401/403, l'utilisateur est "parqué" ici. Le polling P1 est suspendu jusqu'à ce que le token ou le `cache_version` change. P2 continue à fonctionner indépendamment.

### Rides cache

```python
_rides_cache: Dict[(bot_id, tg_id), {
    "rides": {ride_id: (pickup_dt, end_dt)},
    "fetched": bool,
    "last_fetch": float,
    "last_cleanup": float,
}]
```

### Cache runtime utilisateur

```python
_user_runtime_cache: Dict[(bot_id, tg_id), {
    "cache_version": int,
    "data": {tz_name, mobile_headers, filters, class_state, booked_slots, blocked_days, bl_uuid, email, password}
}]
```
Évite de recharger toute la config depuis EC2 à chaque cycle de 300 ms. Invalidé automatiquement quand `cache_version` change.

### Cache "not valid" (anti-spam)

```python
_recent_not_valid_cache: Dict[(bot_id, tg_id, platform, offer_id, cache_version), expiry_ts]
TTL : 60 secondes
```
Après un rejet ou un échec de réservation, l'offre est ignorée pendant 60 s pour éviter de retenter sur la même offre en boucle.

### Portal token en mémoire

```python
_portal_token_mem: Dict[(bot_id, tg_id), (token, exp_monotonic)]
```
Évite une lecture DB à chaque cycle P2 de 200 ms. Expire automatiquement 5 min avant le `exp` du JWT.

### Token-OK flag

```python
_token_ok_mem: Dict[(bot_id, tg_id, cache_version), bool]
```
Après la première réponse 200 de P1, ce flag évite d'appeler `set_token_status("valid")` sur EC2 à chaque cycle.

### Déduplication en mémoire (optionnelle)

```python
accepted_per_user[bot_id][tg_id]: Set[offer_id]    # reset toutes les 24h
rejected_per_user[bot_id][tg_id][platform]: Set[offer_id]  # reset toutes les 1 min
```
Activé par `OFFER_MEMORY_DEDUPE=1`. Désactivé en production pour ne pas rater des offres réutilisées.

---

## 14. Coordination multi-utilisateur

`poller_core/offer_coordinator.py` gère le cas où plusieurs chauffeurs utilisent le système et voient la même offre.

### Principe

```
offer_key = f"{platform}:{offer_id}"

register_candidate(offer_key, bot_id, tg_id, data)
  → Enregistre l'intention de réserver cette offre

claim_offer(offer_key, winner_bot_id, winner_tg_id) → [peer_candidates]
  → Marque l'offre comme prise
  → Retourne les autres candidats qui doivent recevoir "not_accepted"

is_claimed_by_peer(offer_key) → bool
  → Vérifie si un autre utilisateur a déjà pris cette offre
```

### Nettoyage
Les entrées `_pending` et `_claimed` expirent après 180 s (`_TTL_S`). Le nettoyage se déclenche au maximum toutes les 10 s (`_CLEANUP_INTERVAL_S`).

---

## 15. Métriques de performance

`poller_core/metrics.py` collecte des statistiques de latence en temps réel.

```python
observe_ms("p1_fetch_ms", elapsed_ms)      # Latence fetch P1
observe_ms("p2_fetch_ms", elapsed_ms)      # Latence fetch P2
observe_ms("offer_filter_ms", elapsed_ms)  # Temps de filtrage par offre
observe_ms("reserve_rtt_ms", elapsed_ms)   # RTT de réservation
observe_ms("offer_end2end_ms", elapsed_ms) # Latence totale poll→notification
```

Chaque métrique maintient une fenêtre glissante de 4000 valeurs et expose `p50` et `p95`.

---

## 16. Configuration — variables d'environnement

### Variables obligatoires

| Variable | Description |
|----------|-------------|
| `EC2_API_URL` | URL de base de l'API EC2 interne |
| `POLLER_API_KEY` | Clé d'authentification pour l'API EC2 |
| `ASSIGNED_BOT_ID` | Identifiant du bot attribué à ce VPS |
| `ASSIGNED_TELEGRAM_ID` | Telegram ID du chauffeur attribué |

### Variables de polling

| Variable | Défaut | Description |
|----------|--------|-------------|
| `POLL_INTERVAL_S` | `0.3` | Intervalle principal de poll (secondes) |
| `P2_POLL_INTERVAL_S` | `2.0` | Intervalle P2 phase calm |
| `P2_BURST_INTERVAL_S` | `0.8` | Intervalle P2 phase burst |
| `P2_BURST_ACTIVE_DURATION` | `300` | Durée de la phase burst (secondes) |
| `P2_BURST_CALM_DURATION` | `60` | Durée de la phase calm (secondes) |
| `BURST_POLL_INTERVAL_S` | `0.3` | Intervalle burst général (après détection d'offre) |
| `BURST_DURATION_S` | `0` | Durée du burst général (0 = désactivé) |
| `RIDES_REFRESH_INTERVAL_S` | `600` | Rafraîchissement du rides cache (secondes) |

### Variables workers et réseau

| Variable | Défaut | Description |
|----------|--------|-------------|
| `MAX_WORKERS` | `10` | Threads max pour le fetch P1+P2 parallèle |
| `HTTP_POOL_SIZE` | `32` | Taille du pool de connexions HTTP |
| `P1_POLL_TIMEOUT_S` | `8` | Timeout fetch P1 |
| `P2_POLL_TIMEOUT_S` | `8` | Timeout fetch P2 |
| `P1_RESERVE_TIMEOUT_S` | `8` | Timeout réservation P1 |
| `P2_RESERVE_TIMEOUT_S` | `8` | Timeout réservation P2 |

### Variables de debug/diagnostics

| Variable | Défaut | Description |
|----------|--------|-------------|
| `DEBUG_PRINT_OFFERS` | `0` | Affiche chaque offre reçue dans les logs |
| `CF_DEBUG` | `0` | Logs détaillés des custom filters |
| `LOG_OFFERS_PAYLOAD` | `0` | Affiche le payload JSON complet des offres |
| `LOG_RAW_API_RESPONSES` | `0` | Affiche les réponses brutes des APIs |
| `DUMP_RIDES_IN_LOGS` | `0` | Affiche les rides dans les logs |
| `ENABLE_P2` | `1` | Active/désactive P2 |
| `ATHENA_USE_OFFERS_ETAG` | `0` | Utilise ETag HTTP pour P2 (évite retransmission) |
| `OFFER_MEMORY_DEDUPE` | `0` | Déduplication en mémoire des offres |

---

## 17. Démarrage et déploiement

### Prérequis

```bash
pip install -r requirements.txt
# Dépendances : requests, python-dotenv, python-dateutil, playwright
playwright install chromium  # Pour l'auto-refresh PKCE
```

### Lancement

```bash
cp .env.example .env
# Éditer .env avec les vraies valeurs

python main.py
```

### Logs à surveiller au démarrage

```
🚀 Poller started
# Puis toutes les 300ms si une offre est trouvée :
🔔 [P1] Offer abc123 | 145.0 USD | class=Business | poll_age=12ms | PU='...' DO='...'
✅ PASSED [P1] abc123 (2ms) → ✅ Classe véhicule | ✅ Prix min: 145 restants | ...
🚀 [P1] Reserving abc123 | 145.0 USD
✅ [P1] Reserve abc123 → HTTP 201 | 287ms | OK
```

### Comportement en cas de problème de token

```
# Token expiré :
⚠️ P1 [bot_id] poll status=401 | body=...
# Si auto-refresh activé :
⚠️ [AUTO-REFRESH] [bot_id] Token 401 — triggering Playwright login
✅ P1 refresh success for bot_id/tg_id trigger=poll
# Sinon, avertissement épinglé dans Telegram
```

---

## 18. Flux de données complet (diagramme)

```
Blacklane P1 API ◄─── GET /api/v1/chauffeur/offers (300ms)
      │
      ▼
 [offers_p1]  ──── (P1 en parallèle avec P2) ────►  [offers_p2] ◄── Blacklane P2 API
      │                                                    │
      │  (P1 traités immédiatement sans attendre P2)       │
      └──────────────────┐           ┌────────────────────┘
                         ▼           ▼
                  _process_offers_for_user()
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
         Class filter  Price/dist  Blacklists
              │          │          │
              └──────────┼──────────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
         Working hrs  Booked slots  Ride conflicts
              │          │          │
              └──────────┼──────────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
         Custom filters (accept/reject/neutral)
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
        ⛔ REJECTED            ✅ PASSED
              │                     │
              ▼                     ▼
       log + notify         _reserve_executor
       (Telegram)          (fire and forget)
                                    │
                         ┌──────────┴──────────┐
                         ▼                     ▼
                   HTTP 2xx OK           HTTP 4xx/5xx
                         │                     │
                         ▼                     ▼
               add_ride_to_cache()    mark_not_valid_cached()
               + notify "accepted"    + notify "not_accepted"
               + notify peers         + log EC2
               + log EC2
```

---

## Annexe — Modèle d'une offre interne

Toutes les offres, qu'elles viennent de P1 ou P2, sont normalisées dans ce format interne avant filtrage :

```json
{
  "type": "ride",
  "id": "abc123",
  "price": 145.50,
  "currency": "USD",
  "vehicleClass": "Business",
  "_platform": "p1",
  "_poll_ts": 1748260800.123,
  "rides": [
    {
      "type": "transfer",
      "pickupTime": "2026-05-26T14:30:00-04:00",
      "endsAt": "2026-05-26T15:45:00-04:00",
      "pickUpLocation": {
        "name": "YUL",
        "address": "Montréal-Trudeau International Airport, ..."
      },
      "dropOffLocation": {
        "name": "Montréal",
        "address": "1000 Rue De La Gauchetière O, H3B 4W5 Montréal"
      },
      "estimatedDistanceMeters": 22000,
      "estimatedDurationMinutes": 32,
      "flight": { "number": "AC 301" },
      "guestRequests": "Pieces of luggage: 2"
    }
  ]
}
```

P2 utilise deux formats selon l'endpoint :
- **Athena JSON:API** (`/hades/offers`) : objets `data` + `included` avec relations — converti par `_map_portal_offer()`
- **Partner API** (`/api/v1/chauffeur/offers`) : format proche de P1 mais avec quelques différences de nommage — converti par `_map_partner_offer()`

---

*Document généré par analyse statique du code source — bl-poller v2026-05*
