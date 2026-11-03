T = {
    # --- Offer header ---
    "offer_header": {"en": "🔥 New offer - {price} - {icon} {word} {plat}", "fr": "🔥 Nouvelle offre - {price} - {icon} {word} {plat}"},
    "word_accepted": {"en": "Offer Accepted", "fr": "Offre acceptée"},
    "word_not_accepted": {"en": "Not Accepted", "fr": "Non acceptée"},
    "word_not_valid": {"en": "not valid", "fr": "non valide"},
    "word_override": {"en": "valid (override)", "fr": "valide (forcée)"},
    "btn_show_details": {"en": "Show details", "fr": "Voir les détails"},

    # --- Offer details ---
    "lbl_type": {"en": "Type", "fr": "Type"},
    "lbl_class": {"en": "Class", "fr": "Classe"},
    "lbl_price": {"en": "Price", "fr": "Prix"},
    "lbl_flight": {"en": "Flight number", "fr": "Numéro de vol"},
    "lbl_requests": {"en": "Special requests", "fr": "Demandes spéciales"},
    "lbl_distance": {"en": "Distance", "fr": "Distance"},
    "lbl_duration": {"en": "Duration", "fr": "Durée"},
    "lbl_starts": {"en": "Starts at", "fr": "Début"},
    "lbl_ends": {"en": "Ends at", "fr": "Fin"},
    "lbl_pickup": {"en": "Pickup", "fr": "Départ"},
    "lbl_dropoff": {"en": "Dropoff", "fr": "Arrivée"},
    "lbl_reason": {"en": "Reason", "fr": "Raison"},
    "val_transfer": {"en": "transfer", "fr": "transfert"},
    "val_hourly": {"en": "hourly", "fr": "à l'heure"},

    # --- Filter summary ---
    "filters_title": {"en": "🧰 Filters:", "fr": "🧰 Filtres :"},
    "filter_default_name": {"en": "Filter", "fr": "Filtre"},
    "filter_ok": {"en": "ok", "fr": "ok"},
    "filter_failed": {"en": "not met", "fr": "non respecté"},
    "rejected": {"en": "rejected", "fr": "refusé"},
    "accepted": {"en": "accepted", "fr": "accepté"},
    "filters_not_met": {"en": "filters not met", "fr": "filtres non respectés"},
    "forced_accept": {"en": "accepted (custom filter) despite: {reason}", "fr": "accepté (filtre personnalisé) malgré : {reason}"},

    # --- Filter names ---
    "f_class": {"en": "Vehicle class", "fr": "Classe véhicule"},
    "f_hours": {"en": "Working hours", "fr": "Horaires"},
    "f_blocked_days": {"en": "Blocked days", "fr": "Jours bloqués"},
    "f_gap": {"en": "Minimum delay", "fr": "Délai minimal"},
    "f_hourly_min": {"en": "Hourly duration min", "fr": "Durée horaire min"},
    "f_hourly_max": {"en": "Hourly duration max", "fr": "Durée horaire max"},
    "f_custom": {"en": "Custom filters", "fr": "Filtres personnalisés"},
    "f_price_min": {"en": "Price min", "fr": "Prix min"},
    "f_price_max": {"en": "Price max", "fr": "Prix max"},
    "f_dist_min": {"en": "Distance min", "fr": "Distance min"},
    "f_dist_max": {"en": "Distance max", "fr": "Distance max"},
    "f_km_inc_min": {"en": "Included km min", "fr": "Km inclus min"},
    "f_km_inc_max": {"en": "Included km max", "fr": "Km inclus max"},
    "f_pickup_bl": {"en": "Pickup blacklist", "fr": "Liste noire départ"},
    "f_dropoff_bl": {"en": "Dropoff blacklist", "fr": "Liste noire arrivée"},
    "f_flights": {"en": "Blocked flights", "fr": "Vols bloqués"},
    "f_booked_slots": {"en": "Booked slots", "fr": "Créneaux bloqués"},
    "f_conflict": {"en": "Conflict with accepted rides", "fr": "Conflit trajets acceptés"},

    # --- Filter details ---
    "d_class_disabled": {"en": "{otype} '{vc}' disabled", "fr": "{otype} '{vc}' désactivé"},
    "d_hours_out": {"en": "pickup time {t} outside {ws}–{we}", "fr": "heure de départ {t} hors plage {ws}–{we}"},
    "d_hours_in": {"en": "{t} within {ws}–{we}", "fr": "{t} dans {ws}–{we}"},
    "d_day_blocked": {"en": "day {day} blocked (Schedule)", "fr": "jour {day} bloqué (calendrier)"},
    "d_day_allowed": {"en": "{day} allowed", "fr": "{day} autorisé"},
    "d_gap_fail": {"en": "{left} min left < threshold {gap} min", "fr": "{left} min restantes < seuil {gap} min"},
    "d_gap_ok": {"en": "{left} min left", "fr": "{left} min restantes"},
    "d_dur_min": {"en": "{dur} min < min {min} min", "fr": "{dur} min < min {min} min"},
    "d_dur_max": {"en": "{dur} min > max {max} min", "fr": "{dur} min > max {max} min"},
    "d_price_min": {"en": "price {price} < minimum {min}", "fr": "prix {price} < minimum {min}"},
    "d_price_max": {"en": "price {price} > maximum {max}", "fr": "prix {price} > maximum {max}"},
    "d_dist_min": {"en": "distance {dist} km < {min} km", "fr": "distance {dist} km < {min} km"},
    "d_dist_max": {"en": "distance {dist} km > {max} km", "fr": "distance {dist} km > {max} km"},
    "d_pickup_hit": {"en": "pickup contains «{term}»", "fr": "le départ contient «{term}»"},
    "d_dropoff_hit": {"en": "dropoff contains «{term}»", "fr": "l'arrivée contient «{term}»"},
    "d_flight_blocked": {"en": "flight {flight} blocked", "fr": "vol {flight} bloqué"},
    "d_no_flight": {"en": "no flight number", "fr": "aucun numéro de vol"},
    "d_slot_unnamed": {"en": "Unnamed", "fr": "Sans nom"},
    "d_slot_conflict": {"en": "falls in blocked slot «{name}» ({start} → {end})", "fr": "tombe dans le créneau bloqué «{name}» ({start} → {end})"},
    "d_conflict": {"en": "conflicts with an accepted ride ({start} – {end})", "fr": "conflit avec une course acceptée ({start} – {end})"},
    "d_no_conflict": {"en": "no conflict", "fr": "aucun conflit"},

    # --- Custom filters ---
    "cf_airport": {"en": "pickup contains 'airport'", "fr": "le départ contient « aéroport »"},
    "cf_baby_seat": {"en": "baby seat blocked", "fr": "siège bébé bloqué"},
    "cf_price_window": {"en": "price {price} < {min} between {a}-{b}", "fr": "prix {price} < {min} entre {a} et {b}"},

    # --- Reservation failures ---
    "rf_taken": {"en": "Offer already taken by another chauffeur.", "fr": "Offre déjà prise par un autre chauffeur."},
    "rf_409": {"en": "Conflict 409: offer already taken.", "fr": "Conflit 409 : offre déjà prise."},
    "rf_410": {"en": "Offer expired/removed (410): it can no longer be booked.", "fr": "Offre expirée/supprimée (410) : elle n'est plus réservable."},
    "rf_422": {"en": "Offer became invalid (422).", "fr": "Offre devenue invalide (422)."},
    "rf_auth": {"en": "Session expired (HTTP {code}).", "fr": "Session expirée (HTTP {code})."},
    "rf_5xx": {"en": "Blacklane server error (HTTP {code}).", "fr": "Erreur serveur Blacklane (HTTP {code})."},
    "rf_timeout": {"en": "Network timeout during the booking.", "fr": "Délai réseau dépassé pendant la réservation."},
    "rf_network": {"en": "Network error during the booking.", "fr": "Erreur réseau pendant la réservation."},
    "rf_refused": {"en": "Booking refused (HTTP {code}).", "fr": "Réservation refusée (HTTP {code})."},

    # --- Token / account alerts ---
    "warn_no_token": {
        "en": "⚠️ <b>Bot Issue</b>: no mobile session\n\nPlease add your mobile session token.",
        "fr": "⚠️ <b>Problème du bot</b> : aucune session mobile\n\nAjoute ton token de session mobile.",
    },
    "warn_expired": {
        "en": "⚠️ <b>Bot Issue</b>: mobile session expired\n\nPlease update your mobile session token.",
        "fr": "⚠️ <b>Problème du bot</b> : session mobile expirée\n\nMets à jour ton token de session mobile.",
    },
    "btn_add_session": {"en": "➕ Add/Update mobile session", "fr": "➕ Ajouter/Mettre à jour la session mobile"},
    "captcha": {
        "en": "⚠️ <b>Blacklane asks for a human verification (captcha)</b> — the bot can't log in by itself. Please send a new token.",
        "fr": "⚠️ <b>Blacklane demande une vérification humaine (captcha)</b> — le bot ne peut pas se connecter tout seul. Envoie un nouveau token.",
    },
    "token_refreshed": {
        "en": "✅ <b>Token refreshed successfully</b> — bot is back online.",
        "fr": "✅ <b>Token renouvelé avec succès</b> — le bot est de nouveau actif.",
    },
}


def t(lang: str, key: str, **kwargs) -> str:
    entry = T.get(key)
    if entry is None:
        return key
    text = entry.get(lang) or entry["en"]
    return text.format(**kwargs) if kwargs else text
