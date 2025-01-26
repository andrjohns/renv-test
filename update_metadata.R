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

future::plan(future::multisession())
furrr::future_walk2(workspaces$Workspace, workspaces$WorkspaceId,
             function(Workspace, WorkspaceId) {
  message("W: ", Workspace)
}, .options = furrr::furrr_options(scheduling = Inf))
future::plan(future::sequential())
