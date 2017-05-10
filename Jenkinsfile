pipeline {
    agent { docker 'maven' }
    stages {
        stage('Test') {
            steps {
                sh 'echo "easy-mq"'
                sh 'mvn test'
            }
        }
    }
}
