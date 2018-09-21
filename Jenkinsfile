#!/usr/bin/env groovy

pipeline {
    agent any

    stages {
        stage('release') {
            when {
                branch 'master'
            }
            steps {
                script {
                    sh(
                        returnStatus: false,
                        script: '''
                        set -o pipefail
                        virtualenv -p /usr/local/bin/python3.6 venv
                        source venv/bin/activate
                        pip install devpi-client
                        python setup.py sdist
                        devpi use https://python-repo.mc.wgenhq.net
                        devpi login jenkins --password "${DEVPI_PASSWORD}"
                        devpi use /jenkins/amplify-packages
                        devpi upload dist/amplify_aws_utils-*.tar.gz
                        '''
                    )
                }
            }
        }
    }

    post {
        always {
            cleanWs()
        }
        failure {
            slackSend channel: "#cfer-astronauts-core", color: "#F73F3F", message: "Release of amplify_aws_utils failed!"
        }
    }
}
