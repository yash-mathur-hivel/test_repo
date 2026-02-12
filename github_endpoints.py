import logging
from fastapi import APIRouter, Request, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any
import json
import traceback
from datetime import datetime


from app.db.session import get_db
from app.utils.utils import create_repo_if_not_exists, convert_repo_info_to_payload, _extract_pr_files
from app.utils.pr_processor import process_pr
from app.db_crud.hivel_code_review.repo import get_repo_by_url
from app.db_crud.insightly.user_integration import get_integration
from app.db_crud.insightly.repo import get_repo_by_clone_url
from app.db_crud.hivel_code_review.branch_configuration import get_branch_config
from app.db_crud.hivel_code_review.repo_configuration import get_repo_config_by_clone_url
from pydantic import BaseModel, HttpUrl
from app.repositories.webhook_tracking import WebhookTrackingRepository

from app.utils.json_utils import safe_json_dumps
from app.services.github_token_service import get_github_token
from app.services.github.api_interface import GitHubAPIInterface

logger = logging.getLogger(__name__)
router = APIRouter()

class CodeReviewRequest(BaseModel):
    pr_url: HttpUrl
    user_integration_id: int  # Required - ID of the user integration in database

@router.post("/webhooks")
async def github_webhook(
    request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db)
) -> Dict[str, Any]:
    try:
        payload = await request.json()
        if not payload.get("user_integration_id"):
            raise HTTPException(status_code=400, detail="Missing user integration ID")
        logger.info(f"User Integration ID: {payload['user_integration_id']} for Repo: {payload['pull_request']['url']}")
        user_integration = get_integration(db, payload["user_integration_id"])
        access_token = get_github_token(db, payload["user_integration_id"])
        # access_token = user_integration.access_token if user_integration.access_token else user_integration.personal_access_token
        GithubInterface = GitHubAPIInterface(access_token=access_token)


        event_type = request.headers.get("X-GitHub-Event")
        if not event_type:
            raise HTTPException(status_code=400, detail="Missing GitHub event type")

        repo_url = payload.get("repository", {}).get("clone_url")
        if not repo_url:
            raise HTTPException(status_code=400, detail="Missing repository URL")

        repo = get_repo_by_url(db, repo_url)
        if not repo:
            repo = create_repo_if_not_exists(db, payload["user_integration_id"], user_integration.organization_id, payload.get("repository", {}))
        insightly_repo = get_repo_by_clone_url(db, repo.clone_url)
        
        webhook_tracking_id = None #TODO
        # process_pr(GithubInterface, db, pr_info, webhook_tracking_id)

        pr_info = GithubInterface.get_pr_details(
            payload.get("repository", {}).get("owner", {}).get("login"),
            payload.get("repository", {}).get("name"),
            payload.get("number"))
        pr_info = GithubInterface.scm_to_unified(scm_data=pr_info, data_type="pr")

        webhook_repo = WebhookTrackingRepository(db=db)
        tracking_data = {
            "user_integration_id": repo.user_integration_id,
            "organization_id": repo.organization_id,
            "repository_id": repo.id,
            "webhook_type": "github",
            "event_type": event_type,
            "event_action": payload.get("action"),
            "event_sender": payload.get("sender", {}).get("login"),
            "webhook_payload": safe_json_dumps(payload),
            "process_status": "in_progress",
            "process_start_date": datetime.utcnow()
        }
        if event_type == "pull_request":
            pr_data = payload.get("pull_request", {})
            tracking_data.update(
                {
                    "pull_request_url": pr_data.get("html_url"),
                    "pull_request_id": pr_data.get("number"),
                }
            )

        tracking = webhook_repo.create_webhook_tracking(tracking_data)

        # Get repo configuration by clone URL to get the correct repo_id
        repo_config = get_repo_config_by_clone_url(db, repo.clone_url)
        if not repo_config:
            logger.warning(f"Repo configuration not found for clone URL: {repo.clone_url}")
            webhook_repo.update_webhook_tracking(
                tracking.id,
                {
                    "process_status": "completed",
                    "process_end_date": datetime.utcnow(),
                    "error_message": f"Repo configuration not found for {repo.clone_url}"
                }
            )
            return {
                "status": "skipped",
                "reason": "Repo configuration not found"
            }

        # Check if code review is enabled for this branch
        branch_name = pr_info.base_ref  # The target branch of the PR
        branch_config = get_branch_config(db, repo_config.repo_id, branch_name)

        if not branch_config or not branch_config.code_review_enabled:
            logger.info(f"Code review is disabled for branch '{branch_name}' in repo_config.repo_id {repo_config.repo_id}")
            webhook_repo.update_webhook_tracking(
                tracking.id,
                {
                    "process_status": "completed",
                    "process_end_date": datetime.utcnow(),
                    "error_message": f"Code review disabled for branch '{branch_name}'"
                }
            )
            return {
                "status": "skipped",
                "reason": f"Code review is disabled for branch '{branch_name}'"
            }

        pr_review_res = process_pr(
            SCMInterface=GithubInterface,
            db=db,
            pr_info=pr_info,
            repo_id=repo.id,
            insightly_repo_id=insightly_repo.id,
            organization_id=repo.organization_id,
            owner=payload.get("repository", {}).get("owner", {}).get("login"),
            repo_name=payload.get("repository", {}).get("name"),
            webhook_tracking_id=None,
        )

        if pr_review_res["status"] == "error":
            webhook_repo.update_webhook_tracking(
                tracking.id,
                {
                    "process_status": "failed",
                    "process_end_date": datetime.utcnow(),
                    "error_message": pr_review_res.get("error", "Unknown error")
                }
            )
            raise HTTPException(status_code=500, detail=f"Error processing PR: {pr_review_res['error']}")
        elif pr_review_res["status"] == "success":
            webhook_repo.update_webhook_tracking(
                tracking.id,
                {
                    "process_status": "completed",
                    "process_end_date": datetime.utcnow()
                }
            )
            return {
                "status": "success",
                "suggestion_count": pr_review_res.get("suggestions", 0),
                "token_usage": str(pr_review_res.get("token_usage", 0))
            }
        elif pr_review_res["status"] == "skipped":
            webhook_repo.update_webhook_tracking(
                tracking.id,
                {
                    "process_status": "completed",
                    "process_end_date": datetime.utcnow()
                }
            )
            return {
                "status": "skipped",
                "token_usage": str(pr_review_res.get("token_usage", 0))
            }
        else:
            webhook_repo.update_webhook_tracking(
                tracking.id,
                {
                    "process_status": "failed",
                    "process_end_date": datetime.utcnow(),
                    "error_message": f"Unknown status: {pr_review_res['status']}"
                }
            )
            raise HTTPException(status_code=500, detail=f"Unknown status: {pr_review_res['status']}")


    except Exception as e:
        logger.error(f"Error processing GitHub webhook: {e}")
        traceback.print_exc()
        # Update webhook tracking as failed if tracking was created
        if 'tracking' in locals():
            webhook_repo.update_webhook_tracking(
                tracking.id,
                {
                    "process_status": "failed",
                    "process_end_date": datetime.utcnow(),
                    "error_message": str(e)
                }
            )
        raise HTTPException(status_code=500, detail="Error processing GitHub webhook")

@router.post("/agentic/review")
async def agentic_review(request: CodeReviewRequest, db: Session = Depends(get_db)):
    try:
        payload = json.loads(request.json())
        if not payload.get("user_integration_id"):
            raise HTTPException(status_code=400, detail="Missing user integration ID")

        try:
            user_integration = get_integration(db, payload["user_integration_id"])
            access_token = get_github_token(db, payload["user_integration_id"])
            # access_token = user_integration.access_token if user_integration.access_token else user_integration.personal_access_token
            GithubInterface = GitHubAPIInterface(access_token=access_token)
        except Exception as e:
            logger.error(f"Error getting user integration or Github token: {e}")
            raise HTTPException(status_code=500, detail="Error getting Github Token or User Integration")

        split_url = payload["pr_url"].split("/") # keeps robust for onprem URLs
        owner = split_url[-4]
        repo_name = split_url[-3]
        pr_number = split_url[-1]
        logger.info(f"URL Split Owner, Repo Name, PR Number: {owner}, {repo_name}, {pr_number}")

        clone_url = payload["pr_url"].split("/pull")[0] + ".git"

        repo = get_repo_by_url(db, clone_url)
        if not repo:
            logger.info("Repo not found in DB. Creating new")
            repo_info = GithubInterface.get_repo_details(owner, repo_name)
            repo_info_unified = GithubInterface.scm_to_unified(
                scm_data=repo_info, 
                data_type="repo"
            )
            repo_payload = convert_repo_info_to_payload(repo_info_unified)
            repo = create_repo_if_not_exists(
                db, 
                payload["user_integration_id"], 
                user_integration.organization_id, 
                repo_payload
            )
        insightly_repo = get_repo_by_clone_url(db, repo.clone_url)

        try:
            pr_info = GithubInterface.get_pr_details(owner, repo_name, pr_number)
            pr_info = GithubInterface.scm_to_unified(scm_data=pr_info, data_type="pr")
        except Exception as e:
            logger.error(f"Error getting PR details: {e}")
            raise HTTPException(status_code=500, detail=f"Error getting PR details: {e}")
        
        
        
        pr_review_res = process_pr(
            SCMInterface=GithubInterface,
            db=db,
            pr_info=pr_info,
            repo_id=repo.id,
            insightly_repo_id=insightly_repo.id,
            organization_id=repo.organization_id,
            owner=owner,
            repo_name=repo_name,
            webhook_tracking_id=None,
        )

        if pr_review_res["status"] == "error":
            raise HTTPException(status_code=500, detail=f"Error processing PR: {pr_review_res['error']}")
        elif pr_review_res["status"] == "success":
            return {
                "status": "success",
                "suggestion_count": pr_review_res.get("suggestions", 0),
                "token_usage": str(pr_review_res.get("token_usage", 0))
            }
        elif pr_review_res["status"] == "skipped":
            return {
                "status": "skipped",
                "token_usage": str(pr_review_res.get("token_usage", 0))
            }
        else:
            raise HTTPException(status_code=500, detail=f"Unknown status: {pr_review_res['status']}")

        
    except Exception as e:
        error_data = {}
        if owner:
            error_data["owner"] = owner
        if repo_name:
            error_data["repo_name"] = repo_name
        if pr_number:
            error_data["pr_number"] = pr_number
        error_data["error"] = str(e)
        logger.error(json.dumps(error_data, indent=2))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error processing agentic review: {e}")


