#!/bin/bash

# This script sets up the environment for the project.

# Get the python command
PYTHON_CMD=$(command -v python3 || command -v python)
if [ -z "$PYTHON_CMD" ]; then
    echo -e "\033[1;91mError: Python is not installed.\033[0m"
    exit 1
fi
# Check if the script is run from the project root directory
if [ ! -f "requirements.txt" ]; then
    echo -e "\033[1;91mError: This script must be run from the project root directory.\033[0m"
    exit 1
fi
# Check if the script is run in a virtual environment
if [ -n "$VIRTUAL_ENV" ]; then
    echo -e "\033[1;91mError: This script should not be run in a virtual environment.\033[0m"
    exit 1
fi


# Function to explain how to activate and deactivate the environment
explain_activation() {
    echo -e "\033[93mTo activate the environment, run:\033[0m"
    echo -e "\033[94m  source venv/bin/activate\033[0m"
    echo -e "\033[93mTo deactivate the environment, run:\033[0m"
    echo -e "\033[94m  deactivate\033[0m"
}

# Function to print usage information
usage() {
    echo -e "\033[1;93mUsage: \033[0m $0 [options]"
    echo -e "\033[1;93mOptions:\033[0m"
    echo -e "  \033[1;92m-h, --help\033[0m        Show this help message"
    echo -e "  \033[1;92m-b, --build\033[0m       Build the environment"
    echo -e "  \033[1;92m-c, --clean\033[0m      Clean the environment"
    echo -e "  \033[1;92m-s, --status\033[0m     Show the environment status"
    echo -e "  \033[1;92m-v, --version\033[0m    Show the version"
    echo -e "\033[1;93mExample:\033[0m"
    echo -e "  \033[1;92m$0 --build\033[0m       Build the environment"
    echo -e "  \033[1;92m$0 --status\033[0m      Show the environment status"
    echo -e "  \033[1;92m$0 --clean\033[0m      Clean the environment"
    echo -e "  \033[1;92m$0 --version\033[0m    Show the version"
    echo -e "\033[1;93mNote:\033[0m The script must be run from the project root directory."
    echo 
    echo -e "\033[1;93mTo use the enviroment, use the following commands:\033[0m"
    explain_activation
}

# Function to build the environment
build_env() {
    echo -e "\033[1;92mBuilding the environment...\033[0m"
    python -m venv venv || python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt || pip3 install -r requirements.txt
    echo -e "\033[1;92mEnvironment built successfully.\033[0m"
}

# Function to clean the environment
clean_env() {
    if [ -d "venv" ]; then
        echo -e "\033[1;92mCleaning the environment...\033[0m"
        rm -rf venv
        echo -e "\033[1;92mEnvironment cleaned.\033[0m"
    else
        echo -e "\033[1;91mError: Environment not found. Nothing to clean.\033[0m"
    fi
}

# Function to show the environment status
status_env() {
    if [ -d "venv" ]; then
        source venv/bin/activate
        echo -e "\033[1;92mEnvironment status:\033[0m"
        echo -e "\033[1;92m  - Environment: \033[0m venv"
        echo -e "\033[1;92m  - Python version: \033[0m $(python --version || python3 --version)"
        echo -e "\033[1;92m  - Installed packages: \033[0m \n$(pip freeze || pip3 freeze)"
    else
        echo -e "\033[1;91mError: Environment not found. Please build it first.\033[0m"
    fi
}

# Function to show the version
version_env() {
    echo -e "\033[1;92mEnvironment version: \033[0m 1.0.0"
}

# Check if any arguments are passed
if [ $# -eq 0 ]; then
    usage
    exit 1
fi

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -b|--build)
            build_env
            explain_activation
            shift
            ;;
        -c|--clean)
            clean_env
            shift
            ;;
        -s|--status)
            status_env
            shift
            ;;
        -v|--version)
            version_env
            shift
            ;;
        *)
            echo -e "\033[1;91mError: Invalid option '$1'.\033[0m"
            usage
            exit 1
            ;;
    esac
done
