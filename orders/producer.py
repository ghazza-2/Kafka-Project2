"""
kafka_producer.py
==================
Producer Kafka adapté au projet Django (orders/models.py).

Flux complet :
  Ce script  →  order_details  →  transaction_consumer
                                →  order_confirmed
                                →  analytics_consumer
                                →  email_consumer

Lancer depuis la racine du projet :
    python kafka_producer.py --mode stream --rate 3 --duration 120
"""
import json
import time
import random
import argparse
import threading

# ─── Données réalistes ────────────────────────────────────────────────────────

FIRST_NAMES = ["Ahmed", "Sarra", "Mohamed", "Yasmine", "Khalil",
               "Nour", "Amine", "Rim", "Omar", "Farah",
               "Youssef", "Lina", "Tarek", "Hana", "Bilel"]

LAST_NAMES  = ["Ben Ali", "Trabelsi", "Mansouri", "Gharbi", "Ferchichi",
               "Khalfallah", "Bouazizi", "Hamdi", "Jebali", "Maaroufi"]

ADDRESSES   = [
    "12 Avenue Habib Bourguiba, Tunis",
    "7 Rue de Marseille, Sfax",
    "45 Boulevard du 7 Novembre, Sousse",
    "3 Rue Ibn Khaldoun, Monastir",
    "88 Avenue de la République, Bizerte",
    "21 Rue Farhat Hached, Nabeul",
    "5 Avenue de l'Environnement, Ariana",
    "17 Rue de la Liberté, Ben Arous",
]

MENU_ITEMS = [
    ("Burger Classic",     8.5),
    ("Sandwich Thon",      5.0),
    ("Pizza Margherita",  12.0),
    ("Tacos Poulet",       9.0),
    ("Sushi Box",         18.0),
    ("Frites Maison",      4.0),
    ("Salade César",       7.5),
    ("Wrap Légumes",       6.5),
    ("Nuggets x6",         6.0),
    ("Hot Dog",            4.5),
    ("Pasta Bolognese",   10.0),
    ("Risotto Champign.", 11.0),
    ("Tiramisu",           5.5),
    ("Cheesecake",         5.0),
    ("Café Latte",         3.0),
]

NOTES = ["Sans oignon s'il vous plaît", "Livraison urgente",
         "Sonner deux fois", "Laisser devant la porte", "", "", ""]

# ─── Générateur ───────────────────────────────────────────────────────────────

order_counter = 0
counter_lock  = threading.Lock()

def next_id():
    global order_counter
    with counter_lock:
        order_counter += 1
        return order_counter

def random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def random_email(name: str) -> str:
    slug = name.lower().replace(" ", ".").replace("'", "")
    return f"{slug}{random.randint(1, 99)}@gmail.com"

def generate_items():
    picked = random.sample(MENU_ITEMS, k=random.randint(1, 4))
    items  = []
    total  = 0.0
    for name, price in picked:
        qty      = random.randint(1, 3)
        subtotal = round(qty * price, 2)
        total   += subtotal
        items.append({"name": name, "quantity": qty,
                      "price": price, "subtotal": subtotal})
    return items, round(total, 2)

def generate_payload():
    """
    Payload compatible avec TOUS les consumers du projet :

    transaction_consumer attend : order_id, user_id, total_cost,
                                  customer_name, customer_email
    analytics_consumer   attend : order_id, total_cost/total_amount,
                                  customer_name
    email_consumer       attend : order_id, customer_email,
                                  customer_name, total_cost
    Order model Django   attend : customer_name, customer_email,
                                  delivery_address, total_amount
    """
    name         = random_name()
    email        = random_email(name)
    items, total = generate_items()

    return {
        # ── Champs transaction_consumer ──────────────────
        "order_id":         next_id(),
        "user_id":          email,          # fallback customer_id
        "total_cost":       total,

        # ── Champs Order model Django ────────────────────
        "customer_name":    name,
        "customer_email":   email,
        "delivery_address": random.choice(ADDRESSES),
        "total_amount":     total,          # alias de total_cost
        "notes":            random.choice(NOTES),

        # ── Items détaillés ──────────────────────────────
        "items": items,
    }

# ─── Producer Kafka ───────────────────────────────────────────────────────────

KAFKA_BROKER = "localhost:9092"
TOPIC        = "order_details"

def make_producer():
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

def send(producer, payload):
    future = producer.send(TOPIC, value=payload)
    try:
        future.get(timeout=5)
        items_str = ", ".join(i["name"] for i in payload["items"])
        print(
            f"  📦 #{payload['order_id']:>4} | "
            f"{payload['customer_name']:<22} | "
            f"{payload['total_cost']:>8.2f} DT | "
            f"{items_str[:38]}"
        )
    except Exception as e:
        print(f"  ❌ Échec #{payload['order_id']} : {e}")

# ─── Modes ────────────────────────────────────────────────────────────────────

def mode_burst(producer, count=50):
    print(f"\n🚀 BURST — {count} commandes d'un coup\n")
    for _ in range(count):
        send(producer, generate_payload())
    producer.flush()
    print(f"\n✅ {count} commandes envoyées.")

def mode_stream(producer, rate=2.0, duration=60):
    print(f"\n🔄 STREAM — {rate} cmd/s pendant {duration}s\n")
    interval = 1.0 / rate
    end      = time.time() + duration
    sent     = 0
    while time.time() < end:
        send(producer, generate_payload())
        sent += 1
        time.sleep(interval)
    producer.flush()
    print(f"\n✅ {sent} commandes envoyées.")

def mode_wave(producer, waves=5, per_wave=20, pause=10):
    print(f"\n🌊 WAVE — {waves} vagues × {per_wave} cmd, pause {pause}s\n")
    total = 0
    for w in range(1, waves + 1):
        print(f"  ── Vague {w}/{waves} ──")
        for _ in range(per_wave):
            send(producer, generate_payload())
            time.sleep(0.08)
        total += per_wave
        producer.flush()
        if w < waves:
            print(f"  ⏸  Pause {pause}s…\n")
            time.sleep(pause)
    print(f"\n✅ {total} commandes envoyées.")

def mode_concurrent(producer, threads=5, per_thread=10):
    print(f"\n⚡ CONCURRENT — {threads} threads × {per_thread} cmd\n")
    def worker():
        for _ in range(per_thread):
            send(producer, generate_payload())
            time.sleep(random.uniform(0.05, 0.4))
    ts = [threading.Thread(target=worker) for _ in range(threads)]
    for t in ts: t.start()
    for t in ts: t.join()
    producer.flush()
    print(f"\n✅ {threads * per_thread} commandes envoyées.")

# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="🍔 Kafka Order Producer")
    parser.add_argument("--mode",       choices=["burst","stream","wave","concurrent"], default="stream")
    parser.add_argument("--count",      type=int,   default=50,  help="[burst] nb commandes")
    parser.add_argument("--rate",       type=float, default=2,   help="[stream] cmd/s")
    parser.add_argument("--duration",   type=int,   default=60,  help="[stream] durée en secondes")
    parser.add_argument("--waves",      type=int,   default=5,   help="[wave] nb vagues")
    parser.add_argument("--per-wave",   type=int,   default=20,  help="[wave] cmd par vague")
    parser.add_argument("--pause",      type=int,   default=10,  help="[wave] pause entre vagues (s)")
    parser.add_argument("--threads",    type=int,   default=5,   help="[concurrent] nb threads")
    parser.add_argument("--per-thread", type=int,   default=10,  help="[concurrent] cmd par thread")
    args = parser.parse_args()

    print("=" * 65)
    print("   🍔  KAFKA ORDER PRODUCER")
    print(f"   Topic  : {TOPIC}")
    print(f"   Broker : {KAFKA_BROKER}")
    print(f"   Mode   : {args.mode.upper()}")
    print("=" * 65)

    producer = make_producer()
    try:
        if   args.mode == "burst":      mode_burst(producer, args.count)
        elif args.mode == "stream":     mode_stream(producer, args.rate, args.duration)
        elif args.mode == "wave":       mode_wave(producer, args.waves, args.per_wave, args.pause)
        elif args.mode == "concurrent": mode_concurrent(producer, args.threads, args.per_thread)
    except KeyboardInterrupt:
        print("\n⛔ Arrêt manuel.")
    finally:
        producer.flush()
        producer.close()
        print("🔌 Producer fermé.")

if __name__ == "__main__":
    main()