<?php
      function csv_to_array($filename='', $delimiter=',')
      {
          if(!file_exists($filename) || !is_readable($filename)) {
              echo("file not existed!");
              return FALSE;
          }
            
          $header = NULL;
          $data = array();
          if (($handle = fopen($filename, 'r')) !== FALSE)
          {
              while (($row = fgetcsv($handle, 1000, $delimiter)) !== FALSE)
              {
                  if(!$header)
                      $header = $row;
                  else
                      $data[] = array_combine($header, $row);
              }
              fclose($handle);
          }
          return $data;
      }

      function csv_to_string () 
      {
          $data_array = csv_to_array ('sf_geo_records.csv');
          $left = "[";
          $right = "],\n";
          $result_str = "[ ['Lat', 'Long', 'Name'], ";
          
          foreach ($data_array as &$value) {
            $str = "";
            foreach ($value as $key=>$curr) {
              if ($key == "Name")
                $str = $str . $curr . "'";
              else if ($key == 'Long') {
                $str = $str . $curr . ", '";
              }
              else
                $str = $str . $curr . ", ";
            }
            // echo ($str);
            // $str = implode(",", $value);
            $str = $left . $str . $right;
            $result_str = $result_str . $str;
          }
          $result_str = rtrim($result_str, ",\n");
          $result_str = $result_str . "]";
          return $result_str;
      }

      csv_to_string();
?>

<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["map"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        // var data = google.visualization.arrayToDataTable([
        //   ['Lat', 'Long', 'Name'],
        //   [37.4232, -122.0853, 'Work'],
        //   [37.4289, -122.1697, 'University'],
        //   [37.6153, -122.3900, 'Airport'],
        //   [37.4422, -122.1731, 'Shopping']
        // ]);

        var data = google.visualization.arrayToDataTable(
          <?php echo (csv_to_string());?>);
        var map = new google.visualization.Map(document.getElementById('map_div'));
        map.draw(data, {
                          showTip: true, 
                          enableScrollWheel: true,
                          useMapTypeControl: true,
                          colors: [0xFF8747, 0xFFB581, 0xc06000]
                });
        console.log (data);
      }



    </script>
  </head>

  <body>
    <div id="map_div" style="width: 1300px; height: 800px"></div>
    
  </body>
</html>