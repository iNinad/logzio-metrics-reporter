pipeline {
    agent {
        docker {
            image 'python:3.10-slim'
        }
    }
    environment {
        NA_TOKEN = credentials('Logzio_NA01_Token_SearchAPI') // Securely fetch NA token
        EU_TOKEN = credentials('Logzio_EU01_Token_SearchAPI') // Securely fetch EU token
    }
    parameters {
        choice(
            name: 'PLATFORM',
            choices: ['production', 'staging'],
            description: 'Select the platform for querying and collecting metrics.'
        )
        string(
            name: 'DATE',
            defaultValue: "${java.time.LocalDate.now().minusDays(7)}",
            description: 'Enter the base date (YYYY-MM-DD)'
        )
        string(
            name: 'START_TIME',
            defaultValue: '08:00:00Z',
            description: 'Enter the start time (HH:mm:ssZ)'
        )
        string(
            name: 'END_TIME',
            defaultValue: '10:00:00Z',
            description: 'Enter the end time (HH:mm:ssZ)'
        )
        string(
            name: 'DATE_OFFSET_RANGE',
            defaultValue: '8',
            description: 'Number of days before and after the base date'
        )
        string(
            name: 'CONFLUENCE_PAGE',
            defaultValue: 'Logz.io Metrics',
            description: 'Enter the Confluence page name'
        )
        booleanParam(
            name: 'CHECKOUT_OAS_DEPLOYMENT',
            defaultValue: false,
            description: 'Select to checkout oas-deployment repository for customers.yml'
        )
    }
    stages {
        stage('Input Validation') {
            steps {
                echo '[INFO] Validating input parameters...'
                sh '''
                if ! [[ "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
                    echo "[ERROR] Invalid date format. Expected YYYY-MM-DD."
                    exit 1
                fi
                if ! [[ "$START_TIME" =~ ^[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]]; then
                    echo "[ERROR] Invalid start time format. Expected HH:mm:ssZ."
                    exit 1
                fi
                if ! [[ "$END_TIME" =~ ^[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]]; then
                    echo "[ERROR] Invalid end time format. Expected HH:mm:ssZ."
                    exit 1
                fi
                if ! [[ "DATE_OFFSET_RANGE" =~ ^[0-9]+$ ]]; then
                    echo "[ERROR] Invalid time range. Expected a positive number."
                    exit 1
                fi
                '''
            }
        }
        stage('Checkout and Setup') {
            parallel {
                stage('Checkout Repositories') {
                    steps {
                        echo '[INFO] Checking out repository containing script.py...'
                        checkout scm

                        // Optional sparse checkout for oas-deployment repository
                        when {
                            expression { params.CHECKOUT_OAS_DEPLOYMENT }
                        }
                        dir('oas-deployment') {
                            steps {
                                echo '[INFO] Checking out oas-deployment repository for customers.yml...'
                                checkout([$class: 'GitSCM',
                                    branches: [[name: '*/eu01-prd']],
                                    extensions: [
                                        [$class: 'SparseCheckoutPaths', paths: ['customers.yml']]
                                    ],
                                    userRemoteConfigs: [[
                                        url: 'git@git.cias.one:tid/oas-deployment',
                                        credentialsId: 'ciasGitCredentialIdentifier'
                                    ]]
                                ])
                            }
                        }
                    }
                }
                stage('Setup Python Environment') {
                    steps {
                        echo '[INFO] Setting up Python environment...'
                        sh '''
                        python3 -m venv venv
                        . venv/bin/activate
                        pip install --no-cache-dir -r requirements.txt
                        '''
                    }
                }
            }
        }
        stage('Run Script') {
            steps {
                script {
                    withCredentials([usernamePassword(credentialsId: 'CONFLUENCE_IMPORTER', usernameVariable: 'confluenceUser', passwordVariable: 'confluencePassword')]) {
                        def platformValue = params.PLATFORM == 'production' ? 'prd' : 'stg'
                        def customersFile = params.CHECKOUT_OAS_DEPLOYMENT ? 'oas-deployment/customers.yml' : 'customers.yml'
                        echo '[INFO] Running Python script...'
                        sh """
                        . venv/bin/activate
                        python logz_metrics_handler.py \
                            --platform "${platformValue}" \
                            --date ${DATE} \
                            --start_time ${START_TIME} \
                            --end_time ${END_TIME} \
                            --date_offset_range ${DATE_OFFSET_RANGE} \
                            --eu_token "$EU_TOKEN" \
                            --na_token "$NA_TOKEN" \
                            --customers_file "${customersFile}" \
                            --page_title "${CONFLUENCE_PAGE}" \
                            --confluence_username "${confluenceUser}" \
                            --confluence_password "${confluencePassword}"
                        """
                    }
                }
            }
        }
        stage('Archive Results') {
            steps {
                echo '[INFO] Archiving output.csv as a build artifact...'
                archiveArtifacts artifacts: 'output.csv', fingerprint: true
            }
        }
    }
    post {
        always {
            echo '[INFO] Cleaning up workspace...'
            cleanWs()
        }
        success {
            echo '[SUCCESS] Pipeline completed successfully!'
        }
        failure {
            echo '[ERROR] Pipeline failed!'
        }
    }
}
