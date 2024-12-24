# Documents the current libraries in the venv and their versions, then store that into requirements.txt
pip freeze > requirements.txt

# Run tests within the folder tests/
pytest tests/
