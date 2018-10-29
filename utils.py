from os import path
from random import sample
from string import ascii_letters, digits


COUNT_XML_IN_ZIP = 100
ROOT_DIR = path.dirname(path.abspath(__file__))
STORE_DIR = path.join(ROOT_DIR, 'store')
RESULT_DIR = path.join(ROOT_DIR, 'result')
XML_EXTENSION = 'xml'
FIRST_RESULT = '01_id_lvl.csv'
SECOND_RESULT = '02_id_object_names.csv'
LEGAL_CHARS = (ascii_letters + digits + "@-")*5
CHARS_COUNT = 32
ZIP_COUNT = 51
XML_COUNT = 101
START_TEMPLATE = "name='"
END_TEMPLATE = "'/>"


class ValidationError(Exception):
    """An error while validating data."""
    def __init__(self, message, code=None, params=None):
        """The `message` argument can be a single error or list of errors."""
        super(ValidationError, self).__init__(message, code, params)
        if isinstance(message, list):
            self.error_list = []
            for message in message:
                # Normalize strings to instances of ValidationError.
                if not isinstance(message, ValidationError):
                    message = ValidationError(message)
                self.error_list.extend(message.error_list)
        else:
            self.message = message
            self.code = code
            self.params = params
            self.error_list = [self]

    @property
    def message_dict(self):
        return dict(self)

    @property
    def messages(self):
        return list(self)

    def __iter__(self):
        if hasattr(self, 'error_dict'):
            for field, errors in self.error_dict.items():
                yield field, list(ValidationError(errors))
        else:
            for error in self.error_list:
                message = error.message
                if error.params:
                    message %= error.params
                yield str(message)

    def __str__(self):
        return 'ValidationError(%s)' % (list(self))


async def random_xml(root_dir=ROOT_DIR, content=None):
    xml_content = content or open(path.join(root_dir, 'temp.xml')).read()
    full_names = []
    for i in range(1, XML_COUNT):
        id_value = ''.join(sample(LEGAL_CHARS, CHARS_COUNT))
        lvl = str(sample(range(1, XML_COUNT), 1)[0])
        xml_id_lvl = xml_content.replace('unique_random_str', id_value).replace('random_1_100', lvl)
        random_objects = ''
        for i in range(1, sample(range(2, 11), 1)[0]):
            random_str = ''.join(sample(LEGAL_CHARS, CHARS_COUNT))
            random_objects += f"<object name='{random_str}'/>\n"
        objects = f'{random_objects}</objects>\n</root>'
        name = f'{id_value}_{lvl}.xml'
        xml_name = path.join(root_dir, 'tmp', name)
        full_names.append((xml_name, name))
        with open(xml_name, 'w+') as f:
            f.write(xml_id_lvl + objects)
    return full_names


async def write_csv(loaded_file, result):
    # block opening here for ending writing at least verified files
    with open(path.join(result, SECOND_RESULT), 'a+') as f:
        content = (l.rstrip() for l in loaded_file)
        id_value = ''
        for c in content:
            if 'id' in c:
                end = c.find(END_TEMPLATE)
                value = c[c.find(START_TEMPLATE)+len(START_TEMPLATE):end]
                id_value = value.replace("id' value='", "")
            elif 'object name' in c:
                end = c.find(END_TEMPLATE)
                value = c[c.find(START_TEMPLATE) + len(START_TEMPLATE):end]
                f.write(id_value + ';' + value + '\n')


async def write_file_ids(file_names, result):
    with open(path.join(result, FIRST_RESULT), 'w+') as f:
        for name in file_names:
            f.write(name.split('.')[0].replace('_', ';') + '\n')
