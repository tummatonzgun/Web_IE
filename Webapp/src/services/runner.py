class FunctionRunner:
    def run(self, function_name, *args, **kwargs):
        module = __import__(f"src.functions.{function_name}", fromlist=[function_name])
        function = getattr(module, function_name)
        return function(*args, **kwargs)