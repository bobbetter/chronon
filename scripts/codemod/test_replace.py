#!/usr/bin/env python3


import glob

"""
we have tests written in multiple flavors

- extending junit TestCase class
- using @test annotation
- using AnyFunSuite
- using AnyFlatSpec
- using vertx junit runner

bazel silently fails to run the tests when they are not uniform!

This script translates almost all of the tests to AnyFlatSpec except for vertx tests.

NOTE: CWD needs to be the root of the repo.

USAGE: python3 scripts/codemod/test_replace.py
"""


def get_test_class_name(path):
    # Get the file name from the path
    filename = path.split("/")[-1]
    # Remove 'Test.scala' and return
    return filename.replace("Test.scala", "")


def convert_fun_suite_to_flatspec(lines, test_name):
    modified_lines = []

    for line in lines:
        # Replace import statement
        if "import org.scalatest.funsuite.AnyFunSuite" in line:
            line = line.replace("funsuite.AnyFunSuite", "flatspec.AnyFlatSpec")
            modified_lines.append(line)
            continue

        # Replace AnyFunSuite with AnyFlatSpec
        if "extends AnyFunSuite" in line:
            line = line.replace("AnyFunSuite", "AnyFlatSpec")
            modified_lines.append(line)
            continue

        # Handle ignore tests and regular tests
        if ("ignore(" in line or "test(" in line) and "{" in line:
            start = line.find('"')
            end = line.find('"', start + 1)
            if start != -1 and end != -1:
                test_desc = line[start + 1 : end]  # Get description without quotes
                words = test_desc.split()

                # Check if second word is "should"
                if len(words) > 1 and words[1].lower() == "should":
                    subject = words[0]  # Use first word as subject
                    remaining_desc = " ".join(
                        words[2:]
                    )  # Rest of description including "should"
                    new_desc = f'"{subject}" should "{remaining_desc}"'
                else:
                    new_desc = f'  it should "{test_desc}"'

                # Add appropriate suffix based on whether it's ignore or test
                if "ignore(" in line:
                    new_line = f"{new_desc} ignore {{"
                else:
                    new_line = f"{new_desc} in {{"

                modified_lines.append(new_line + "\n")
                continue

        # Keep other lines unchanged
        modified_lines.append(line)

    return "".join(modified_lines)


def split_camel_case(word):
    if not word:
        return []

    result = []
    current_word = word[0].lower()

    for i in range(1, len(word)):
        current_char = word[i]
        prev_char = word[i - 1]

        # Split on transition from lowercase to uppercase
        if current_char.isupper() and prev_char.islower():
            result.append(current_word)
            current_word = current_char.lower()
        # Split on transition from uppercase to lowercase, but only if it's not
        # part of an acronym (i.e., if the previous char was also uppercase and
        # not at the start of a word)
        elif (
            current_char.islower()
            and prev_char.isupper()
            and i > 1
            and word[i - 2].isupper()
        ):
            result.append(current_word[:-1])
            current_word = prev_char.lower() + current_char
        else:
            current_word += current_char.lower()

    result.append(current_word)
    return [token for token in result if token != "test"]


def convert_junit_to_flatspec(lines, test_name):
    modified_lines = []
    is_test_method = False
    class_modified = False

    for line in lines:
        # Replace JUnit import with FlatSpec import
        if "import org.junit.Test" in line:
            modified_lines.append("import org.scalatest.flatspec.AnyFlatSpec\n")
            continue

        # Handle class definition
        if "class" in line and "Test" in line and (not class_modified):
            class_modified = True
            class_name = line.split("class")[1].split("{")[0].strip()
            modified_lines.append(f"class {class_name} extends AnyFlatSpec {{\n")
            continue

        # Mark start of a test method
        if "@Test" in line:
            is_test_method = True
            continue

        # Convert only test methods marked with @Test and not private
        if (
            is_test_method
            and "def " in line
            and "private" not in line
            and (("(): Unit" in line) or ("): Unit" not in line))
        ):
            is_test_method = False

            method_name = line.split("def ")[1].split("(")[0]

            test_description = " ".join(split_camel_case(method_name))

            modified_lines.append(f'  it should "{test_description}" in {{\n')
            continue

        is_test_method = False
        modified_lines.append(line)

    return "".join(modified_lines)


def convert_testcase_to_flatspec(lines, test_name):
    modified_lines = []

    for line in lines:
        # Replace TestCase import with FlatSpec import
        if "junit.framework.TestCase" in line:
            modified_lines.append("import org.scalatest.flatspec.AnyFlatSpec\n")
            continue

        # Handle imports that we want to keep
        if line.startswith("import") and "TestCase" not in line:
            modified_lines.append(line)
            continue

        # Handle class definition
        if "class" in line and "extends TestCase" in line:
            class_name = line.split("class")[1].split("extends")[0].strip()
            modified_lines.append(f"class {class_name} extends AnyFlatSpec {{\n")
            continue

        # Convert test methods (they start with "def test")
        if (
            "def test" in line
            and "private" not in line
            and ("(): Unit" in line or "): Unit" not in line)
        ):
            method_name = line.split("def test")[1].split("(")[0].strip()
            # If there are parameters, capture them

            test_description = " ".join(split_camel_case(method_name))

            modified_lines.append(f'  it should "{test_description}" in {{\n')
            continue

        modified_lines.append(line)

    return "".join(modified_lines)


def convert(handler, file_path):
    test_name = get_test_class_name(file_path)
    with open(file_path, "r") as file:
        lines = file.readlines()
        converted = handler(lines, test_name)

    with open(file_path, "w") as file:
        file.write(converted)

    print(f"Converted {file_path}")


# Few challenging test cases below

# convert(
#     convert_junit_to_flatspec,
#     "spark/src/test/scala/ai/chronon/spark/test/JoinUtilsTest.scala",
# )

# convert(
#     convert_junit_to_flatspec,
#     "spark/src/test/scala/ai/chronon/spark/test/LocalExportTableAbilityTest.scala",
# )

# convert(
#     convert_testcase_to_flatspec,
#     "aggregator/src/test/scala/ai/chronon/aggregator/test/FrequentItemsTest.scala",
# )

# convert(
#     convert_fun_suite_to_flatspec,
#     "spark/src/test/scala/ai/chronon/spark/test/FetcherTest.scala",
# )


if __name__ == "__main__":
    test_files = glob.glob("**/*Test.scala", recursive=True)

    fun_suite_files = []
    junit_files = []
    others = []
    junit_test_case_files = []
    flat_spec_files = []

    for file_path in test_files:
        try:
            with open(file_path, "r") as file:
                content = file.read()
                if "AnyFunSuite" in content:
                    fun_suite_files.append(file_path)
                elif "import org.junit.Test" in content:
                    junit_files.append(file_path)
                elif "extends TestCase" in content:
                    junit_test_case_files.append(file_path)
                elif "extends AnyFlatSpec" in content:
                    flat_spec_files.append(file_path)
                else:
                    others.append(file_path)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    print(f"funsuite files:\n   {"\n   ".join(fun_suite_files)}")

    for file in fun_suite_files:
        convert(convert_fun_suite_to_flatspec, file)

    print(f"junit files:\n   {"\n   ".join(junit_files)}")

    for file in junit_files:
        convert(convert_junit_to_flatspec, file)

    print(f"test case files:\n   {"\n   ".join(junit_test_case_files)}")

    for file in junit_test_case_files:
        convert(convert_testcase_to_flatspec, file)

    print(f"flat spec files:\n   {"\n   ".join(flat_spec_files)}")
    print(f"Other files:\n   {"\n   ".join(others)}")
