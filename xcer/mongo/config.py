"""Config YAML operations with MongoDB."""

import glob
import json
import time
from pathlib import Path
from pymongo import MongoClient
from xcer.paths import CONFIG_FOLDER, CONFIG_HASH_FILE


def get_config_yaml(client: MongoClient, field: str, verbose=False) -> str:
    """Get config YAML content, using local cache if hash matches."""
    filename = f"{field}.yaml"
    local_config_file = CONFIG_FOLDER / filename

    hash_changed = check_hash_changed(client, verbose=verbose)
    if hash_changed:
        if verbose:
            print("Config hash changed, will download from MongoDB.")
        should_download = True
    elif not local_config_file.exists():
        if verbose:
            print(
                f"Local config file {local_config_file} not found, will download from MongoDB."
            )
        should_download = True
    else:
        if verbose:
            print(
                f"Local config file {local_config_file} exists and hash matches, using local file."
            )
        should_download = False

    if should_download:
        download_config_yaml(client, verbose=verbose)

    if local_config_file.exists():
        with open(local_config_file, "r") as f:
            return f.read()
    else:
        raise ValueError(
            f"Config file {local_config_file} not found after downloading from MongoDB. This may indicate the requested field {field} is invalid. Or the last config upload wasn't uploaded (xcer apply)."
        )


def check_hash_changed(client: MongoClient, verbose=False) -> bool:
    """Check if local config hash matches MongoDB hash."""
    db = client["config_db"]
    hash_collection = db["config_hash"]
    hash_doc = hash_collection.find_one(
        {"_id": "current_config"}, {"_id": 0, "hash": 1}
    )
    if hash_doc is None:
        raise ValueError("No config hash document found in MongoDB.")

    if CONFIG_HASH_FILE.exists():
        with open(CONFIG_HASH_FILE, "r") as f:
            local_hash_doc = json.load(f)
        if local_hash_doc["hash"] == hash_doc["hash"]:
            if verbose:
                print(
                    f"Local config hash {local_hash_doc['hash']} matches MongoDB hash "
                    f"{hash_doc['hash']}."
                )
            return False
        else:
            if verbose:
                print(
                    f"Local config hash {local_hash_doc['hash']} does not match "
                    f"MongoDB hash {hash_doc['hash']}."
                )
            return True
    else:
        if verbose:
            print(f"Local config hash file {CONFIG_HASH_FILE} not found, ")
        return False


def upload_config_yaml(client: MongoClient, verbose=False):
    """Upload config YAML files to MongoDB using replace operations."""
    db = client["config_db"]

    # Upload config hash
    hash_collection = db["config_hash"]
    config_collection = db["config"]
    yaml_files = sorted(glob.glob(str(CONFIG_FOLDER / "*.yaml")))
    yaml_filename = [Path(file).name for file in yaml_files]
    all_config_texts = {f: open(f, "r").read() for f in yaml_files}
    combined_text = "\n\n".join(all_config_texts[f] for f in yaml_files)

    config_hash_doc = {
        "_id": "current_config",  # Use fixed ID for singleton document
        "hash": hash(combined_text),
        "filenames": yaml_filename,
        "upload_time": str(__import__("datetime").datetime.now()),
        "upload_from": __import__("socket").gethostname(),
    }

    # Remove all old configs first (they're invalid for the new hash)
    delete_result = config_collection.delete_many({})
    result = hash_collection.replace_one(
        {"_id": "current_config"}, config_hash_doc, upsert=True
    )
    if not result.acknowledged:
        raise RuntimeError("Failed to upload config hash to MongoDB.")
    if verbose:
        print(
            f"Successfully uploaded config hash {config_hash_doc['hash']} to MongoDB."
        )

    # Upload config yaml files
    # Then upload the new configs (using insert_many since collection is empty)
    config_docs = []
    for yaml_file in yaml_files:
        filename = Path(yaml_file).name
        config_doc = {
            "_id": filename,  # Use filename as unique ID
            "filename": filename,
            "content": all_config_texts[yaml_file],
        }
        config_docs.append(config_doc)

    if config_docs:
        result = config_collection.insert_many(config_docs)
        if not result.acknowledged or len(result.inserted_ids) != len(config_docs):
            raise RuntimeError("Failed to upload config yaml files to MongoDB.")

    if verbose:
        print(
            f"Config yaml upload complete: deleted {delete_result.deleted_count} old configs, "
            f"uploaded {len(yaml_files)} new configs: {', '.join([Path(f).name for f in yaml_files])}"
        )


def download_config_yaml(client: MongoClient, verbose=False):
    """Download config YAML files from MongoDB to local cache."""
    target_folder = CONFIG_FOLDER
    CONFIG_FOLDER.mkdir(parents=True, exist_ok=True)
    db = client["config_db"]

    max_retries = 3
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            # Download hash document
            hash_collection = db["config_hash"]
            hash_doc = hash_collection.find_one({"_id": "current_config"})
            if hash_doc is None:
                if attempt < max_retries - 1:
                    if verbose:
                        print(
                            f"Config hash not found (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay}s..."
                        )
                    time.sleep(retry_delay)
                    continue
                else:
                    raise ValueError("No config hash document found in MongoDB.")

            with open(CONFIG_HASH_FILE, "w") as f:
                f.write(json.dumps(hash_doc, indent=2))

            # Download yaml files
            config_collection = db["config"]
            yaml_docs = list(config_collection.find({}))

            # Check if configs are missing (race condition during upload)
            if not yaml_docs or len(yaml_docs) != len(hash_doc["filenames"]):
                if attempt < max_retries - 1:
                    if verbose:
                        print(
                            f"Config files temporarily missing (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay}s..."
                        )
                    time.sleep(retry_delay)
                    continue
                else:
                    raise ValueError("No config documents found in MongoDB.")

            for doc in yaml_docs:
                filename = doc["filename"]
                content = doc["content"]
                with open(target_folder / filename, "w") as f:
                    f.write(content)

            print(
                f"Config yaml download complete. Version hash: {hash_doc['hash']}, "
                f"files: {', '.join(hash_doc['filenames'])}, uploaded at {hash_doc['upload_time']} "
                f"from {hash_doc['upload_from']}."
            )
            return  # Success, exit retry loop

        except Exception as e:
            if attempt < max_retries - 1:
                if verbose:
                    print(
                        f"Download failed (attempt {attempt + 1}/{max_retries}): {e}, retrying in {retry_delay}s..."
                    )
                time.sleep(retry_delay)
                continue
            else:
                raise  # Re-raise on final attempt
