# Working with the KB

This KB is mostly `Markdown`, but it has embedded PlantUML charts.
These usually don't work out-of-the-box and might require a little additional configuration.

To check the correect rendering of embedded charts open [this file](test_embeds.md).
If your editor supports this, and everything is configured correctly,
you should see a rendered charts.

Here are two options you can use to work with this KB

## PyCharm

- Install `graphviz`.
- Install and enable the Markdown plugin in PyCharm.

In theory this should be enough, but you may find that charts give you a rendering error
about not finding "dot".
In this case find the `dot` executable in your system and copy it to `/opt/local/bin/dot`.
It should work now.

## Obsidian

- Install the `Obsidian` app
- Install and enable the PlantUML plugin in Obsidian.
  You might need to configure the path to the dot executable.
- Open the kb folder as a vault
