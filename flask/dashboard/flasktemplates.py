from random import choice
from string import lowercase, uppercase, digits

from paste.script.templates import Template
from paste.script.templates import var

CHARSET = lowercase+uppercase+digits


class FlaskTemplate(Template):
    vars = [
    ]
    use_cheetah = True
    required_templates = []

    def __init__(self, name):
        self.append_secret_key()
        super(FlaskTemplate, self).__init__(name)

    def check_vars(self, vars, command):
        if not command.options.no_interactive and not hasattr(command, '_deleted_once'):
            del vars['package']
            command._deleted_once = True
        return super(FlaskTemplate, self).check_vars(vars, command)

    def append_secret_key(self):
        default_key = ''.join(choice(CHARSET) for i in range(50))
        self.vars.append(var('secret_key', 'Secret key', default=default_key))


class FlaskBasicTemplate(FlaskTemplate):
    _template_dir = 'templates/flask_basic'
    summary = 'Flask basic project'


class FlaskSQLAlchemyTemplate(FlaskTemplate):
    _template_dir = 'templates/flask_sqla'
    summary = 'Flask project using SQLAlchemy'
    vars = [
        var('db_uri', 'Database URI', default='sqlite:///:memory:'),
    ]
