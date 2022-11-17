"""Checks to run against a submission."""
from __future__ import print_function

import json
import os

import constants
import mlp_compliance.mlp_compliance as mlp_compliance
import report as subm_report

INFINITE_TIME = 9999999999.99


class SubmissionChecks(object):
  """Submission checks."""

  def __init__(self):
    self.report = subm_report.SubmissionReport()
    self.submission_meta = {}
    self.result_meta = {}
    self.result_entry_meta = {}

  def verify_dirs_and_files(self, root_dir):
    self.verify_root_dir(root_dir)
    self.verify_code_dir(root_dir)
    self.verify_results_dir(root_dir)

  def verify_metadata(self):
    """Verifies the metadata.

        Must be called after verify_dirs_and_files()
    """
    self.verify_submission_metadata()
    self.verify_result_entry_metadata()

  def exists(self, path, is_dir=False):
    exists_fn = os.path.isdir if is_dir else os.path.isfile
    if exists_fn(path):
      self.report.add_passed_check(f'Path exists: {path}')
    else:
      self.report.add_failed_check(f'Path not found: {path}')

  def name_in(self, path, ref_list):
    basename = os.path.basename(path)
    if basename in ref_list:
      self.report.add_passed_check(f'{path} name is in {ref_list}.')
    else:
      self.report.add_failed_check(f'{path} name not in {ref_list}.')

  def keys_match(self, keys, ref_keys, context=''):
    if different_keys := set(keys).difference(set(ref_keys)):
      self.report.add_failed_check(f'Keys in {context} do not match expected: ' +
                                   f'unmatched keys: {list(different_keys)}')
    else:
      self.report.add_passed_check(f'Keys in {context} match expected.')

  def verify_root_dir(self, root_dir):
    result_dir = os.path.join(root_dir, 'results')
    code_dir = os.path.join(root_dir, 'code')
    submission_meta_file = os.path.join(root_dir, 'submission.json')

    self.exists(result_dir, is_dir=True)
    self.exists(code_dir, is_dir=True)
    self.exists(submission_meta_file, is_dir=False)

    try:
      with open(submission_meta_file) as f:
        self.submission_meta = json.load(f)
    except Exception as e:
      self.report.add_error(f'Unable to parse submission meatadata: {str(e)}')

  def verify_code_dir(self, root_dir):
    code_root_dir = os.path.join(root_dir, 'code')
    try:
      for code_name in os.listdir(code_root_dir):
        code_dir = os.path.join(code_root_dir, code_name)
        if not os.path.isdir(code_dir):
          continue
        self.name_in(code_dir, constants.BENCHMARK_NAMES + ['shared'])
        if code_name in constants.BENCHMARK_NAMES:
          self.exists(os.path.join(code_dir, 'README.md'))
          self.exists(os.path.join(code_dir, 'preproc_dataset.sh'))
    except Exception as e:
      self.report.add_error(f'Unable to verify code dir: {str(e)}')

  def verify_results_dir(self, root_dir):
    code_root_dir = os.path.join(root_dir, 'code')
    result_root_dir = os.path.join(root_dir, 'results')
    try:
      for entry_name in os.listdir(result_root_dir):
        entry_dir = os.path.join(result_root_dir, entry_name)
        if not os.path.isdir(entry_dir):
          continue
        entry_meta_file = os.path.join(entry_dir, 'entry.json')
        try:
          with open(entry_meta_file) as f:
            self.result_entry_meta[entry_name] = json.load(f)
        except Exception as e:
          self.report.add_error(f'Unable to parse result entry metadata: {str(e)}')
        self.exists(entry_meta_file)
        for result_name in os.listdir(entry_dir):
          result_dir = os.path.join(entry_dir, result_name)
          if not os.path.isdir(result_dir):
            continue
          self.name_in(result_dir, constants.BENCHMARK_NAMES)
          self.exists(os.path.join(code_root_dir, result_name, f'setup_{entry_name}.sh'))
          self.exists(
              os.path.join(code_root_dir, result_name,
                           f'run_and_time_{entry_name}.sh'))
          result_num = constants.REQUIRED_RESULT_NUM.get(result_name, 0)
          for i in range(result_num):
            log_file_name = f'result_{str(i)}.txt'
            self.exists(os.path.join(result_dir, log_file_name))
            self.result_meta.setdefault(entry_name, {})
            self.result_meta[entry_name].setdefault(
                result_name, [None for _ in range(result_num)])
            division = self.result_entry_meta[entry_name].get('division')
            log_path = os.path.join(result_dir, log_file_name)
            dt, start_time = self.verify_and_extract_time(log_path,
                                                          division,
                                                          result_name)
            self._add_result(self.result_meta[entry_name][result_name],
                             i,
                             dt,
                             start_time)
    except Exception as e:
      self.report.add_error(f'Unable to verify results dir: {str(e)}')

  def _add_result(self, dict_entry, entry, dt, start_time):
    """Adds a result to the dictionary.

    Args:
      dict_entry: main dict to add entry
      entry: slot for this entry (likely an integer)
      dt: the timing for the entry
      start_time: when the entry started unix time float
    """
    time_entry = {'dt': dt, 'start_time': start_time}
    dict_entry[entry] = time_entry

  def _sorted_results(self, results_dicts):
    """Sorts dict of results based on log start_time.

    Sorts the results and returns an array with only the values but sorted
    by oldest value first.value

    Args:
      results_dicts: List of result dicts

    Returns:
      List of only the time but sorted oldest first.
    """
    print('results dicts:', results_dicts)
    sorted_dict = sorted(results_dicts, key=lambda k: k['start_time'])
    return [entry['dt'] for entry in sorted_dict]

  def verify_submission_metadata(self):
    subm_meta_keys = self.submission_meta.keys()
    if different_keys := set(subm_meta_keys).difference(
        set(constants.SUBM_META_PROPS)):
      self.report.add_failed_check(
          f'Keys in submission metadata do not match expected: unmatched keys: {list(different_keys)}'
      )
    else:
      self.report.add_passed_check(
          'Keys in submission metadata match expected.')

  def verify_result_entry_metadata(self):
    for entry_name in self.result_entry_meta:
      entry_meta = self.result_entry_meta[entry_name]
      entry_meta_keys = entry_meta.keys()
      self.keys_match(
          entry_meta_keys,
          constants.ENTRY_META_PROPS,
          context=f'entry {entry_name} metadata',
      )
      try:
        for node_meta in entry_meta['nodes']:
          node_meta_keys = node_meta.keys()
          self.keys_match(
              node_meta_keys,
              constants.NODE_META_PROPS,
              context=f'entry {entry_name} node metadata',
          )
      except Exception as e:
        self.report.add_error(
            f'Unable to verify node metadata for entry {entry_name}: {str(e)}')

  def compile_results(self):
    results = {}
    for entry_name in self.result_meta:
      results.setdefault(entry_name, {})
      for key in constants.RESULT_SUBM_META_COLUMNS:
        results[entry_name][key] = self.submission_meta[key]
      for key in constants.RESULT_ENTRY_META_COLUMNS:
        results[entry_name][key] = self.result_entry_meta[entry_name][key]
      for benchmark_name in constants.BENCHMARK_NAMES:
        benchmark_results = self.result_meta[entry_name].get(
            benchmark_name, None)
        if not benchmark_results:
          results[entry_name][benchmark_name] = None
          continue
        if not all(benchmark_results):
          self.report.add_error(
              f'Benchmark results contain None values. entry: {entry_name}, benchmark name: {benchmark_name}'
          )
          results[entry_name][benchmark_name] = None
          continue
        # Turns dict into list or results oldest first
        benchmark_results = self._sorted_results(benchmark_results)
        # special treatment for the NCF results
        if benchmark_name == 'ncf':
          possible_results = benchmark_results
          benchmark_results = []
          for pr in possible_results:
            # Skips results that were failures to converge, which are set to
            # INFINITE_TIME
            if pr != INFINITE_TIME:
              benchmark_results.append(pr)
            # Stops after it finds 50 results, which may not be the first
            # 50 by date unless they are sorted earlier.
            if len(benchmark_results) == 50:
              break
          if len(benchmark_results) != 50:
            raise Exception('NCF does not have 50 good results')
          if len(possible_results) != 100:
            raise Exception(f'NCF does not have 100 good results:{len(possible_results)}')

        benchmark_results = sorted(benchmark_results)
        print(
            f'benchmark_name:{benchmark_name}|{entry_name} results{benchmark_results}'
        )
        del benchmark_results[0]
        del benchmark_results[-1]
        result_val = (
            float(sum(benchmark_results)) / len(benchmark_results) /
            constants.REFERENCE_RESULTS[benchmark_name])
        results[entry_name][benchmark_name] = result_val
    self.report.set_results(results)

  def get_compliance(self, filename):
    """Get the compliance level of the output file."""
    print(f'Running Compliance Check on {filename}')
    print('#' * 80)
    start_time, status, dt, qual, target = mlp_compliance.l2_check_file_w_starttime(
        filename)
    print('#' * 80)

    if status:
      level = '2'
    else:
      start_time, status, dt, qual, target = mlp_compliance.l1_check_file_w_starttime(
          filename)
      print('#' * 80)
      level = '1' if status else '0'
    success = status and qual and target and qual >= target
    return start_time, level, dt, qual, success

  def verify_and_extract_time(self, log_file, division, result_name):
    """Verifies and result and returns timing.

    Uses submodule mlp_compliance (https://github.com/bitfort/mlp_compliance)

    Args:
      log_file: Absolute path to result file.
      division: open, closed
      result_name: name of the benchmark, ncf, ssd, etc

    Returns:
      Time for the result or `INFINITE_TIME` if not a success

    Raises:
      Exception: If expected compliance level is not hit or cannot figure
      out expected compliance level.

    """
    expected_level = constants.DIVISION_COMPLIANCE_CHECK_LEVEL.get(
        division, None)
    print(result_name)
    if expected_level is None:
      raise Exception(f'Unknown division: {division}')
    start_time, level, dt, _, success = self.get_compliance(log_file)
    print(float(start_time))
    if int(level) != expected_level:
      raise Exception(
          f'Error Level {level} does not match needed level {expected_level}:{log_file}'
      )

    if success and dt:
      return dt, start_time
    print(f'Result was not a success set to INFINITE_TIME({INFINITE_TIME})')
    return INFINITE_TIME, start_time
