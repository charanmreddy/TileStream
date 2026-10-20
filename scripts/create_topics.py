"""Create the 3 Kafka topics and print their partition layout.
Run from the project root:  python -m scripts.create_topics"""
from common import config
from common import kafka_utils as ku


def main():
    print(f"Connecting to Kafka at {config.BOOTSTRAP} ...")
    created = ku.ensure_topics()
    print("Newly created topics:", created or "none (already existed)")
    print()
    print(f"{'topic':<12}{'partition':<11}{'leader':<8}{'replicas':<10}{'isr'}")
    for topic, parts in ku.describe_topics().items():
        for pid, leader, replicas, isr in parts:
            print(f"{topic:<12}{pid:<11}{leader:<8}{str(replicas):<10}{isr}")


if __name__ == "__main__":
    main()
