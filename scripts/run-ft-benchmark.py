import argparse
import re
import os
import subprocess
import tempfile
import shutil

LOCK_FILE = "/tmp/run-ft-benchmark.lock"
REPO_URL = "https://github.com/near/nearcore"

def parse_time(time_str):
    match = re.match(r'^(\d+)([hms])$', time_str)
    if not match:
        raise argparse.ArgumentTypeError(f"Invalid time format: '{time_str}'. Must be a number followed by 'h', 'm', or 's'.")
    return match.group(1) + match.group(2)

def parse_state(state_str):
    match = re.match(r'^(\d+)([KMG]?)$', state_str)
    if not match:
        raise argparse.ArgumentTypeError(f"Invalid state format: '{state_str}'. Must be a number optionally followed by 'K', 'M', or 'G'.")
    return match.group(1) + match.group(2)

def create_lock_file(user):
    if os.path.exists(LOCK_FILE):
        with open(LOCK_FILE, 'r') as f:
            running_user = f.read().strip()
        raise RuntimeError(f"{running_user} already running benchmark")
    with open(LOCK_FILE, 'w') as f:
        f.write(user)

def remove_lock_file():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

def run_command(command, cwd=None):
    result = subprocess.run(command, shell=True, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command '{command}' failed with error: {result.stderr}")
    return result.stdout.strip()

def checkout_repo(commit):
    temp_dir = tempfile.mkdtemp()
    repo_dir = os.path.join(temp_dir, "nearcore")
    
    try:
        run_command(f"git config --global http.postBuffer 524288000")
        run_command(f"git clone {REPO_URL} {repo_dir}")
        run_command("git fetch", cwd=repo_dir)
        if not commit:
            commit = run_command("git rev-parse origin/master", cwd=repo_dir)
        run_command(f"git checkout {commit}", cwd=repo_dir)
    except RuntimeError as e:
        shutil.rmtree(temp_dir)
        raise e
    
    return repo_dir, commit

def run_benchmark(repo_dir, time, users, state, shards, nodes, rump_up, user):
    benchmark_command = (
        f"./scripts/start_benchmark.sh {time} {users} {state} {shards} {nodes} {rump_up} {user}"
    )
    run_command(benchmark_command, cwd=repo_dir)

def main():
    parser = argparse.ArgumentParser(description="Run FT benchmark")
    parser.add_argument('--time', type=parse_time, default='1h', help="Time duration (e.g., 2h, 30m, 45s, default: 1h)")
    parser.add_argument('--users', type=int, default=100, help="Number of users (positive integer, default: 100)")
    parser.add_argument('--state', type=parse_state, default='1G', help="State size (e.g., 1000, 500K, 200M, 1G, default: 1G)")
    parser.add_argument('--shards', type=int, default=1, help="Number of shards (integer, default: 1)")
    parser.add_argument('--nodes', type=int, default=1, help="Number of nodes (integer, default: 1)")
    parser.add_argument('--rump-up', type=int, default=5, help="Rump-up time (integer, default: 5)")
    parser.add_argument('--user', type=str, default='default_user', help="User name (string, default: 'default_user')")
    parser.add_argument('--commit', type=str, help="Commit hash (string, default: latest commit on master)")

    args = parser.parse_args()

    try:
        create_lock_file(args.user)
        print(f"Time: {args.time}")
        print(f"Users: {args.users}")
        print(f"State: {args.state}")
        print(f"Shards: {args.shards}")
        print(f"Nodes: {args.nodes}")
        print(f"Rump-up: {args.rump_up}")
        print(f"User: {args.user}")

        repo_dir, commit = checkout_repo(args.commit)
        print(f"Using commit: {commit}")

        run_benchmark(repo_dir, args.time, args.users, args.state, args.shards, args.nodes, args.rump_up, args.user)

    except RuntimeError as e:
        print(e)
    finally:
        remove_lock_file()
        if 'repo_dir' in locals() and os.path.exists(repo_dir):
            shutil.rmtree(os.path.dirname(repo_dir))

if __name__ == "__main__":
    main()
