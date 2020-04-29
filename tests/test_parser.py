import unittest

from wikidict.parser import Parser


class TestParser(unittest.TestCase):
    def test_remove_templates(self):
        source = "moi {{aa}} moi"
        self.assertEqual("moi  moi", Parser(source).remove_templates().content)

    def test_remove_templates_nested(self):
        source = "{{a {{b}} }}xyz"
        self.assertEqual("xyz", Parser(source).remove_templates().content)

    def test_remove_templates_multiple(self):
        source = "{{a {{{{c}} b}} }}xyz{{aa}}a"
        self.assertEqual("xyza", Parser(source).remove_templates().content)

    def test_get_first_paragraph(self):
        source = '{{template}} testContent \n\n==Next section header==\n:any content here'
        self.assertEqual("testContent", Parser(source).remove_templates().get_first_section().content)

    def test_replace_links(self):
        source = "hii [[moi]][[x lol]]"
        self.assertEqual("hii [moi](#moi)[x lol](#x-lol)", Parser(source).to_markdown().content)

    def test_replace_links_target(self):
        source = "[[moi|hips]][[x lol|prii proo]] yy"
        self.assertEqual("[hips](#moi)[prii proo](#x-lol) yy", Parser(source).to_markdown().content)


if __name__ == '__main__':
    unittest.main()
