renv::activate()
source("utilities.R")
source("write_dataflow.R")
source("write_dataset.R")

tk <- AzureAuth::get_azure_token(resource = "https://analysis.windows.net/powerbi/api",
                                  tenant = Sys.getenv("az_tenant_id"),
                                  app = Sys.getenv("az_app_id_pbi_dataset"),
                                  password = Sys.getenv("az_cli_secret_pbi_dataset"))
tk$refresh()
access_token <- tk$credentials$access_token

workspaces <- qiverse.powerbi::list_workspaces(access_token)

# Remove any folders if Workspace no longer exists
curr_dirs <- list.dirs(recursive = FALSE)
curr_dirs <- curr_dirs[!(curr_dirs %in% c("./.git", "./.github"))]
removed <- curr_dirs[!(curr_dirs %in% paste0("./", workspaces$Workspace))]

if (length(removed) > 0) {
  purrr::walk(removed, function(dir) { unlink(dir, recursive = TRUE) })
}


future::plan(future::multisession())
furrr::future_walk2(workspaces$Workspace, workspaces$WorkspaceId,
             function(Workspace, WorkspaceId) {
  message("W: ", Workspace)
  if (!dir.exists(Workspace)) {
    dir.create(Workspace)
  }
  ## Dataflows
  dataflows <- qiverse.powerbi:::list_dataflows(WorkspaceId, access_token)

  ### Remove folders for any deleted dataflows
  curr_dirs <- list.dirs(path = file.path(Workspace, "Dataflows"), recursive = FALSE)
  removed <- curr_dirs[!(curr_dirs %in% file.path(Workspace, "Dataflows", dataflows$Dataflow))]
  if (length(removed) > 0) {
    purrr::walk(removed, function(dir) { unlink(dir, recursive = TRUE) })
  }

  if (isTRUE(nrow(dataflows) > 0)) {
    purrr::walk2(dataflows$Dataflow, dataflows$DataflowId, write_dataflow,
                 Workspace, WorkspaceId, access_token)
  } else if (length(curr_dirs) > 0) {
    # No longer any dataflows in workspace, so remove all folders
    unlink(file.path(Workspace, "Dataflows"), recursive = TRUE)
  }

  ## Datasets
  datasets <- qiverse.powerbi:::list_datasets(Workspace, access_token)

  ### Remove folders for any deleted datasets
  curr_dirs <- list.dirs(path = file.path(Workspace, "Datasets"), recursive = FALSE)
  removed <- curr_dirs[!(curr_dirs %in% file.path(Workspace, "Datasets", datasets$Dataset))]
  if (length(removed) > 0) {
    purrr::walk(removed, function(dir) { unlink(dir, recursive = TRUE) })
  }
  if (isTRUE(nrow(datasets) > 0)) {
    purrr::walk2(datasets$Dataset, datasets$DatasetId, write_dataset,
                 Workspace, access_token)
  } else if (length(curr_dirs) > 0) {
    # No longer any datasets in workspace, so remove all folders
    unlink(file.path(Workspace, "Datasets"), recursive = TRUE)
  }
}, .options = furrr::furrr_options(scheduling = Inf))
future::plan(future::sequential())
