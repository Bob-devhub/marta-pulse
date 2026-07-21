"""fabric-cicd entrypoint used by the CD pipeline.

Publishes everything under fabric/ to the target workspace, applying
per-environment find/replace from deploy/parameter.yml (connection GUIDs,
lakehouse IDs, Eventstream endpoints differ across dev/test/prod).
"""

import argparse

from azure.identity import DefaultAzureCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

ITEM_TYPES = [
    "Lakehouse",
    "Eventhouse",
    "KQLDatabase",
    "Eventstream",
    "Notebook",
    "DataPipeline",
    "KQLDashboard",
    "Reflex",           # Activator
    "SemanticModel",
    "Report",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace-id", required=True)
    ap.add_argument("--environment", required=True, choices=["dev", "test", "prod"])
    ap.add_argument("--repo-dir", default="fabric")
    args = ap.parse_args()

    ws = FabricWorkspace(
        workspace_id=args.workspace_id,
        repository_directory=args.repo_dir,
        item_type_in_scope=ITEM_TYPES,
        environment=args.environment,
        # Required by current fabric-cicd. On the runner this resolves via the
        # azure/login OIDC session (AzureCliCredential); locally via az login.
        token_credential=DefaultAzureCredential(),
    )
    publish_all_items(ws)
    unpublish_all_orphan_items(ws, item_name_exclude_regex="^DO_NOT_DELETE.*")


if __name__ == "__main__":
    main()
