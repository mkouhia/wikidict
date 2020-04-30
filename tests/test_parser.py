import unittest

from wikidict.parser import Parser


class TestParser(unittest.TestCase):
    def test_remove_templates(self):
        source = 'test {{aa}} test'
        self.assertEqual('test  test', Parser(source).remove_templates().content)

    def test_remove_templates_nested(self):
        source = '{{a {{b}} }}xyz'
        self.assertEqual('xyz', Parser(source).remove_templates().content)

    def test_remove_templates_multiple(self):
        source = '{{a {{{{c}} b}} }}hedge{{aa}}hog'
        self.assertEqual('hedgehog', Parser(source).remove_templates().content)

    def test_get_first_paragraph(self):
        source = '{{template}} testContent \n\n==Next section header==\n:any content here'
        self.assertEqual('testContent', Parser(source).remove_templates().get_first_section().content)

    def test_replace_links(self):
        source = 'string [[test]][[x lol]]'
        self.assertEqual('string [test](#test)[x lol](#x-lol)', Parser(source).to_markdown().content)

    def test_replace_links_target(self):
        source = '[[test|bed]][[x lol|word another]] yy'
        self.assertEqual('[bed](#test)[word another](#x-lol) yy', Parser(source).to_markdown().content)


if __name__ == '__main__':
    unittest.main()
