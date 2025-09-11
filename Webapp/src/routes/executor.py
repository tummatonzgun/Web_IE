from flask import Blueprint, request, jsonify
from src.services.runner import FunctionRunner

executor_bp = Blueprint('executor', __name__)
function_runner = FunctionRunner()

@executor_bp.route('/execute', methods=['POST'])
def execute_function():
    data = request.json
    function_name = data.get('function_name')
    params = data.get('params', {})

    try:
        result = function_runner.run(function_name, **params)
        return jsonify({'result': result}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400