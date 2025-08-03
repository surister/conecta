import copy
import dataclasses
import time
import re
import subprocess

import tqdm


@dataclasses.dataclass
class Library:
    """
    Represents a Python library to be used in a benchmark case.

    Attributes:
        name: Name of the library.
        version: Version of the library (e.g. 'latest', '1.2.3').
        extra_deps: Optional list of additional dependencies (e.g. ['pyarrow', 'pandas==2.0.1']).
    """
    name: str
    version: str
    extra_deps: list = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class BenchCase:
    """
    Represents a benchmark case to be executed.

    Attributes:
        name: Name or identifier of the benchmark case.
        times: Number of times the case should be run.
        library: Library object with details about dependencies.
        func: Fully qualified name of the function to benchmark (e.g., 'module.function').
        func_params: Parameters to pass to the function.
        environment: Environment variables to set during execution.
        results_capture: Dictionary with keys like 'total_memory' and 'total_elapsed',
            each mapped to a lambda function that extracts values from logs.
    """

    name: str
    times: int
    library: Library
    func: str
    func_params: dict = dataclasses.field(default_factory=dict)
    environment: dict = dataclasses.field(default_factory=dict)
    results_capture: dict = dataclasses.field(default_factory=dict)

    def runnable_func(self):
        """
        Constructs a string of Python code that imports and runs the target function.

        Example:
            "from module import func as f ; f(**{'param_1': 1, 'param_2': 2})"

        Returns:
            str: A Python statement suitable for subprocess execution.
        """
        module, func = self.func.split('.')
        return f'from {module} import {func} as f; f(**{self.func_params})'


@dataclasses.dataclass
class BenchCaseResult:
    """
    Stores the result of a benchmark execution.

    Attributes:
        case: The benchmark case that was run.
        total_elapsed: Time the tested function took to run.
        total_memory: Peak memory usage.
        times_run: Number of times the test was run.
        test_total_elapsed: Total wall-clock time spent running the case.
        logs: The logs that were produced on the benchmark run.
    """
    case: BenchCase
    total_elapsed: float
    total_memory: float
    times_run: int
    test_total_elapsed: float
    logs: str


def setattr_nested(obj, attr_path, value):
    parts = attr_path.split('.')
    for part in parts[:-1]:
        obj = getattr(obj, part)

    if isinstance(obj, dict):
        obj[parts[-1]] = value
    # setattr(obj, parts[-1], value)


@dataclasses.dataclass
class repeat_with:
    name: str
    other_case_name: str
    new_params: dict = dataclasses.field(default_factory=dict)

    def get_case_from(self, case: BenchCase) -> BenchCase:
        case.name = self.name

        for k, v in self.new_params.items():
            setattr_nested(case, k, v)
        return case


DEFAULT_CASES = [
    BenchCase(
        name='conecta-4-lineitem_small',
        library=Library(name='conecta', version='latest', extra_deps=['pyarrow']),
        func="conecta.read_sql",
        func_params=dict(
            conn="postgres://postgres:postgres@192.168.88.251:5400",
            queries=['select * from lineitem_small'],
            partition_on='l_orderkey',
            partition_num=4,
            return_backend='pyarrow'
        ),
        environment={
            'RUST_LOG': 'conecta=debug'
        },
        times=1,
        results_capture={
            'total_memory': lambda logs: re.search(
                r'Peak memory usage:\s*([\d.]+MB)', logs).group(1),
            'total_elapsed': lambda logs: re.search(
                r'Finished loading data.*?since Checkpoint\[\d+\]: ([\d.]+(?:ms|s|µs))',
                logs).group(1)
        }
    ),
    repeat_with(
        name='conecta-4-lineitem',
        other_case_name='conecta-4-lineitem_small',
        new_params={
            'func_params.queries': ['select * from lineitem']
        }
    ),
]


def get_case(name: str):
    case = [case for case in DEFAULT_CASES if case.name == name.strip()]

    if case:
        case = case[0]
        if isinstance(case, repeat_with):
            case = case.get_case_from(case=copy.deepcopy(get_case(case.other_case_name)))
    else:
        raise Exception(f'Case {name} is not defined')
    return case

def run_case(case: BenchCase) -> BenchCaseResult:
    """
    Executes a single benchmark case using a subprocess and returns the result.

    Args:
        case (BenchCase): The benchmark case to run.

    Returns:
        BenchCaseResult: Result of the benchmark including memory, timing, and environment info.
    """
    t = time.time()
    p = subprocess.run(
        ['uv', 'run', '--with', ",".join([case.library.name, *case.library.extra_deps]), 'python',
         '-c', case.runnable_func()],
        env=case.environment,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    logs = p.stderr.decode()

    return BenchCaseResult(
        case=case,
        total_elapsed=time.time() - t if 'total_elapsed' not in case.results_capture else
        case.results_capture['total_elapsed'](logs),
        total_memory=case.results_capture['total_memory'](logs),
        times_run=case.times,
        test_total_elapsed=time.time() - t,
        logs=logs
    )


def run_benchmark(benchmark_plan: str) -> list[BenchCaseResult]:
    results = []
    plan = benchmark_plan.split(',')

    cases = [get_case(i) for i in plan]

    for case in tqdm.tqdm(cases, desc='Running benchmark'):
        result = run_case(case)
        results.append(result)
    return results


# Example running
if __name__ == '__main__':
    bench_plan = "conecta-4-lineitem_small, conecta-4-lineitem"
    results = run_benchmark(benchmark_plan=bench_plan)
