from userstats import is_identifiable


class TestIsIdentifiable:
    def test_regular_danish_names(self):
        assert is_identifiable("Anders Bendtsen")
        assert is_identifiable("Claus Degn Eriksen")

    def test_with_emojis(self):
        assert is_identifiable("Anders Bendtsen 💙💛")
        assert is_identifiable("Claus💛 Degn💛 Eriksen💛")

    def test_lower_case(self):
        assert is_identifiable("anders bendtsen")
        assert is_identifiable("claus degn eriksen")

    def test_camel_case(self):
        assert is_identifiable("AndersBendtsen")
        assert is_identifiable("ClausDegnEriksen")

    def test_foreign_names(self):
        assert is_identifiable("Arthur Nilsson")  # Swedish last name
        assert not is_identifiable("Arthur Cameron")  # English name

    def test_single_name(self):
        assert not is_identifiable("Anders")
        assert not is_identifiable("Claus")

    def test_single_name_with_identifiers(self):
        assert not is_identifiable("Anders Fra Venstre")

    def test_initials(self):
        assert not is_identifiable("Anders B")
        assert not is_identifiable("Claus D. E")

    def test_1337_writing(self):
        assert not is_identifiable("4nd3rs b3ndt$3n")
        assert not is_identifiable("Cl4us Degn Eriksen")
        assert is_identifiable("Claus |)39/V Eriksen")  # sufficient material

    def test_non_names(self):
        assert not is_identifiable("cool_guy13")
        assert not is_identifiable("nedmedsystemet")

