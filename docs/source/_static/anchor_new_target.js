<!-- Adds target=_blank to external links -->
$(document).ready(function () {
  $('a[href^="http://"], a[href^="https://"]').not('a[class*=internal]').attr('target', '_blank');
});