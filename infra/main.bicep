// MARTA Pulse — Azure-side infrastructure (everything outside Fabric).
// Fabric items are deployed by fabric-cicd, not Bicep.
targetScope = 'resourceGroup'

@allowed(['dev', 'test', 'prod'])
param env string
param location string = resourceGroup().location
param railApiUrl string = 'https://developerservices.itsmarta.com:18096/itsmarta/railrealtimearrivals/developerservices/traindata'
param busVpUrl string
param busTuUrl string
param eventstreamEntityName string

var suffix = 'martapulse-${env}'

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: replace('st${suffix}', '-', '')
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: { minimumTlsVersion: 'TLS1_2' }
}

resource kv 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: 'kv-${suffix}'
  location: location
  properties: {
    sku: { family: 'A', name: 'standard' }
    tenantId: tenant().tenantId
    enableRbacAuthorization: true
    // Secrets set out-of-band: rail-api-key, eventstream-connection
  }
}

resource insights 'Microsoft.Insights/components@2020-02-02' = {
  name: 'appi-${suffix}'
  location: location
  kind: 'web'
  properties: { Application_Type: 'web' }
}

// Container Flex Consumption uses for zip deployments
resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource deployContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'deployments'
}

// Flex Consumption (FC1) — successor to Linux Consumption (Y1), which
// reaches EOL 2028-09-30. Still Linux-based (Python requires Linux), but
// actively supported, with identity-based storage and per-instance scaling.
resource plan 'Microsoft.Web/serverfarms@2024-04-01' = {
  name: 'plan-${suffix}'
  location: location
  kind: 'functionapp'
  sku: { name: 'FC1', tier: 'FlexConsumption' }
  properties: { reserved: true }
}

resource func 'Microsoft.Web/sites@2024-04-01' = {
  name: 'func-${suffix}'
  location: location
  kind: 'functionapp,linux'
  identity: { type: 'SystemAssigned' }
  properties: {
    serverFarmId: plan.id
    functionAppConfig: {
      runtime: { name: 'python', version: '3.11' }
      deployment: {
        storage: {
          type: 'blobContainer'
          value: '${storage.properties.primaryEndpoints.blob}deployments'
          authentication: { type: 'SystemAssignedIdentity' }
        }
      }
      scaleAndConcurrency: {
        maximumInstanceCount: 40   // FC1 minimum; a 15s timer barely needs 1
        instanceMemoryMB: 2048
      }
    }
    siteConfig: {
      appSettings: [
        // Identity-based storage: no account keys in app settings
        { name: 'AzureWebJobsStorage__accountName', value: storage.name }
        { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: insights.properties.ConnectionString }
        { name: 'BUS_VP_URL', value: busVpUrl }
        { name: 'BUS_TU_URL', value: busTuUrl }
        { name: 'RAIL_API_URL', value: railApiUrl }
        { name: 'RAIL_API_KEY', value: '@Microsoft.KeyVault(VaultName=${kv.name};SecretName=rail-api-key)' }
        { name: 'EVENTSTREAM_CONNECTION', value: '@Microsoft.KeyVault(VaultName=${kv.name};SecretName=eventstream-connection)' }
        { name: 'EVENTSTREAM_NAME', value: eventstreamEntityName }
      ]
    }
    httpsOnly: true
  }
  dependsOn: [deployContainer]
}

// Grant the Function's managed identity Key Vault Secrets User
resource kvRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(kv.id, func.id, 'kv-secrets-user')
  scope: kv
  properties: {
    principalId: func.identity.principalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
    principalType: 'ServicePrincipal'
  }
}

// Storage Blob Data Owner: required for identity-based AzureWebJobsStorage
// (timer-trigger leases) AND for Flex Consumption deployment storage.
resource storageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, func.id, 'blob-data-owner')
  scope: storage
  properties: {
    principalId: func.identity.principalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b')
    principalType: 'ServicePrincipal'
  }
}

output functionAppName string = func.name
output keyVaultName string = kv.name
