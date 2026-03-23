"""
Concurrent annotation test: N users each get an element and annotate it every second.

Verifies that multiple users can annotate simultaneously without conflicts.

Usage: python test_api_concurrent_annotations.py [--users N] [--duration T]
  N: number of concurrent users (default: 3)
  T: duration in seconds (default: 30)
"""

import argparse
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from atclient.automate import (
    check,
    create_test_project,
    create_test_user,
    delete_test_project,
    delete_test_user,
    load_api,
    load_test_data,
)
from atclient.pyactivetigger import AtApi

USER_PASSWORD = "Annotator1!"


def annotator_worker(admin_api, user_index, data, slug, scheme, labels, results, ready_event, stop_event):
    """Single user workload: get next element, annotate it, repeat every second."""
    tag = f"[user-{user_index}]"
    username = None
    user_api = None

    try:
        # 1. Admin creates a user account
        username = create_test_user(
            admin_api, prefix=f"annot{user_index}", password=USER_PASSWORD
        )
        print(f"{tag} Created user: {username}")
        results[user_index]["username"] = username

        # 2. Grant user access to the shared project
        admin_api.add_auth_user_project(username, slug)
        print(f"{tag} Granted access to '{slug}'")

        # 3. User connects with their own session
        user_api = AtApi(url=admin_api.url)
        user_api.connect(username, USER_PASSWORD)
        if user_api.headers is None:
            raise Exception(f"{tag} User authentication failed")
        print(f"{tag} Authenticated")
        results[user_index]["user_api"] = user_api

        # Signal ready
        ready_event.set()

        # 4. Annotation loop: get element + annotate every second
        count = 0
        errors = 0
        label_index = user_index % len(labels)

        while not stop_event.is_set():
            try:
                element = user_api.get_next_element(slug, scheme)
            except Exception as e:
                errors += 1
                print(f"{tag} Error getting next element: {e}")
                time.sleep(1)
                continue

            element_id = element["element_id"]
            label = labels[label_index % len(labels)]
            label_index += 1

            try:
                user_api.post_annotation(slug, scheme, element_id, label)
                count += 1
                if count % 10 == 0:
                    print(f"{tag} Annotated {count} elements so far")
            except Exception as e:
                errors += 1
                print(f"{tag} Error annotating {element_id}: {e}")

            time.sleep(1)

        results[user_index]["count"] = count
        results[user_index]["errors"] = errors
        print(f"{tag} Done: {count} annotations, {errors} errors")

    except Exception as e:
        print(f"{tag} SETUP ERROR: {e}")
        results[user_index]["error"] = str(e)
        ready_event.set()


def cleanup(admin_api, results):
    """Tear down all user accounts created during the test."""
    print("\n--- Cleanup ---")
    for i, res in sorted(results.items()):
        tag = f"[user-{i}]"
        username = res.get("username")
        if username:
            delete_test_user(admin_api, username)
            print(f"{tag} Deleted user {username}")


def main():
    parser = argparse.ArgumentParser(description="Concurrent annotation test")
    parser.add_argument(
        "--users", type=int, default=3, help="Number of concurrent users (default: 3)"
    )
    parser.add_argument(
        "--duration", type=int, default=30, help="Duration in seconds (default: 30)"
    )
    args = parser.parse_args()

    n_users = args.users
    duration_sec = args.duration

    print(f"=== Concurrent Annotation Test: {n_users} users, {duration_sec}s ===\n")

    admin_api = load_api()
    ping = admin_api.ping()
    check(ping["available"], "API is reachable")

    # Load data and create a shared project with labels
    data = load_test_data()
    print("Creating shared project...")
    slug, state = create_test_project(admin_api, data=data, force_label=True, n_train=1000, n_test=100)
    print(f"  Project ready: {slug}")

    # Get scheme and labels
    schemes = admin_api.get_schemes(slug)
    scheme = list(schemes.keys())[0]
    labels = schemes[scheme]["labels"]
    print(f"  Scheme: {scheme}, Labels: {labels}")

    results = {i: {} for i in range(n_users)}
    ready_events = [threading.Event() for _ in range(n_users)]
    stop_event = threading.Event()

    # Launch annotator threads
    threads = []
    for i in range(n_users):
        t = threading.Thread(
            target=annotator_worker,
            args=(admin_api, i, data, slug, scheme, labels, results, ready_events[i], stop_event),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Wait for all workers to be ready
    for evt in ready_events:
        evt.wait(timeout=120)

    n_ready = sum(1 for r in results.values() if r.get("user_api"))
    n_err = sum(1 for r in results.values() if r.get("error"))
    print(f"\n=== All users ready: {n_ready}/{n_users} (errors: {n_err}) ===")
    print(f"Annotating for {duration_sec}s...\n")

    # Let them annotate for the requested duration
    try:
        time.sleep(duration_sec)
    except KeyboardInterrupt:
        print("\nInterrupted by user.")

    # Signal stop and join
    stop_event.set()
    for t in threads:
        t.join(timeout=10)

    # Report
    print("\n=== Results ===")
    total_annotations = 0
    total_errors = 0
    for i, res in sorted(results.items()):
        count = res.get("count", 0)
        errors = res.get("errors", 0)
        total_annotations += count
        total_errors += errors
        print(f"  [user-{i}] annotations: {count}, errors: {errors}")

    print(f"\n  Total annotations: {total_annotations}")
    print(f"  Total errors: {total_errors}")

    # Cleanup users then project
    cleanup(admin_api, results)
    delete_test_project(admin_api, slug)
    print(f"  Deleted project {slug}")

    print(f"\n=== Test finished ===")
    check(total_annotations > 0, "At least one annotation was made successfully.")
    check(total_errors == 0, "No annotation errors occurred.")


if __name__ == "__main__":
    main()
