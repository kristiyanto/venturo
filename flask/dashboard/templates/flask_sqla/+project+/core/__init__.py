
def setup_routing(app, routes):
    """
    Registers blueprint instances and add routes all at once.
    Routes are defined using the following format:
    [
    ((blueprint_or_app_instance, url_prefix),
        ('/route1/<param>', view_function),
        ('/route2', ViewClass.as_view('view_name'),
    ),
    ...
    ]
    """
    for route in routes:
        endpoint, rules = route[0], route[1:]
        for pattern, view in rules:
            if endpoint is None:
                app.add_url_rule(pattern, view_func=view)
            else:
                endpoint[0].add_url_rule(pattern, view_func=view)
        if endpoint is not None:
            app.register_blueprint(endpoint[0], url_prefix=endpoint[1])
