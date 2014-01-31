# Copyright 2009-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""MongoDB specific extensions to Sphinx."""

from docutils import nodes
from sphinx import addnodes
from sphinx.util.compat import (Directive,
                                make_admonition)


class mongodoc(nodes.Admonition, nodes.Element):
    pass


class mongoref(nodes.reference):
    pass


def visit_mongodoc_node(self, node):
    self.visit_admonition(node, "seealso")


def depart_mongodoc_node(self, node):
    self.depart_admonition(node)


def visit_mongoref_node(self, node):
    atts = {"class": "reference external",
            "href": node["refuri"],
            "name": node["name"]}
    self.body.append(self.starttag(node, 'a', '', **atts))


def depart_mongoref_node(self, node):
    self.body.append('</a>')
    if not isinstance(node.parent, nodes.TextElement):
        self.body.append('\n')


class MongodocDirective(Directive):

    has_content = True
    required_arguments = 0
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = {}

    def run(self):
        return make_admonition(mongodoc, self.name,
                               ['See general MongoDB documentation'],
                               self.options, self.content, self.lineno,
                               self.content_offset, self.block_text,
                               self.state, self.state_machine)


def process_mongodoc_nodes(app, doctree, fromdocname):
    for node in doctree.traverse(mongodoc):
        anchor = None
        for name in node.parent.parent.traverse(addnodes.desc_signature):
            anchor = name["ids"][0]
            break
        if not anchor:
            for name in node.parent.traverse(nodes.section):
                anchor = name["ids"][0]
                break
        for para in node.traverse(nodes.paragraph):
            tag = str(para.traverse()[1])
            link = mongoref("", "")
            link["refuri"] = "http://dochub.mongodb.org/core/%s" % tag
            link["name"] = anchor
            link.append(nodes.emphasis(tag, tag))
            new_para = nodes.paragraph()
            new_para += link
            node.replace(para, new_para)


def setup(app):
    app.add_node(mongodoc,
                 html=(visit_mongodoc_node, depart_mongodoc_node),
                 latex=(visit_mongodoc_node, depart_mongodoc_node),
                 text=(visit_mongodoc_node, depart_mongodoc_node))
    app.add_node(mongoref,
                 html=(visit_mongoref_node, depart_mongoref_node))

    app.add_directive("mongodoc", MongodocDirective)
    app.connect("doctree-resolved", process_mongodoc_nodes)
