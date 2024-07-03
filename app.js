// ABCD_15: Updated Node.js Server to Handle Except Emails and Job Title Levels EOT001db34939
const express = require('express');
const fileUpload = require('express-fileupload');
const csv = require('fast-csv');
const mysql = require('mysql2');
//const csv = require('csv-parser');
const bodyParser = require('body-parser');
const XLSX = require('xlsx');
const fs = require('fs-extra');
const path = require('path');
const util = require('util');
const app = express();
const port = 3000;

// MySQL Connection
const db = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'yoda7'
});


// Directory for storing uploads
const uploadsDir = path.join(__dirname, 'uploads');
fs.ensureDirSync(uploadsDir); // Ensure the directory exists

db.connect(err => {
  if (err) throw err;
  console.log('Connected to MySQL database');
});

// Body Parser Middleware
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

// Serve static files from the "public" directory
app.use(express.static('public'));

// Search endpoint
app.post('/search', (req, res) => {
  const names = req.body.names.split('\n').map(name => name.trim()).filter(name => name);
  const emails = req.body.emails.split('\n').map(email => email.trim()).filter(email => email);
  const exceptEmails = req.body.exceptEmails.split('\n').map(email => email.trim()).filter(email => email);
  const jobTitleLevels = req.body.jobTitleLevels;
  const jobtitles = req.body.jobtitles.split('\n').map(jobtitle => jobtitle.trim()).filter(jobtitle => jobtitle);
  const job_title_role = req.body.job_title_role.split('\n').map(role => role.trim()).filter(role => role);
  const job_title_sub_role = req.body.job_title_sub_role.split('\n').map(subRole => subRole.trim()).filter(subRole => subRole);
  const job_company_name = req.body.job_company_name.split('\n').map(job_company_name => job_company_name.trim()).filter(job_company_name => job_company_name);
  const job_company_website = req.body.job_company_website.split('\n').map(website => website.trim()).filter(website => website);
  const exclude_job_company_name = req.body.exclude_job_company_name.split('\n').map(exclude_company => exclude_company.trim()).filter(exclude_company => exclude_company);
  const exclude_job_company_website = req.body.exclude_job_company_website.split('\n').map(exclude_website => exclude_website.trim()).filter(exclude_website => exclude_website);
  const mobile_phone = req.body.mobile_phone.split('\n').map(phone => phone.trim()).filter(phone => phone);
  const phone_numbers = req.body.phone_numbers.split('\n').map(phone => phone.trim()).filter(phone => phone);
  const countries = req.body.countries;
  const job_company_size = req.body.job_company_size;
  const job_company_industry = req.body.job_company_industry;
  const job_company_type = req.body.job_company_type;
  const linkedin_connections = req.body.linkedin_connections;
  const job_start_date = req.body.job_start_date;
  const inferred_years_experience = req.body.inferred_years_experience;
  const job_company_founded = req.body.job_company_founded;

  
  


  let conditions = [];

  if (names.length > 0) {
    const nameConditions = names.map(name => `(full_name LIKE '%${name}%' OR first_name LIKE '%${name}%' OR last_name LIKE '%${name}%')`).join(' OR ');
    conditions.push(`(${nameConditions})`);
  }

  if (emails.length > 0) {
    const emailConditions = emails.map(email => `(work_email LIKE '%${email}%' OR FIND_IN_SET('${email}', emails))`).join(' OR ');
    conditions.push(`(${emailConditions})`);
  }

  if (exceptEmails.length > 0) {
    const exceptEmailConditions = exceptEmails.map(email => `work_email NOT LIKE '%${email}%'`).join(' AND ');
    conditions.push(`(${exceptEmailConditions})`);
  }

  if (jobTitleLevels && jobTitleLevels.length > 0) {
    const jobTitleLevelConditions = jobTitleLevels.map(level => `(job_title_levels LIKE '%${level}%')`).join(' OR ');
    conditions.push(`(${jobTitleLevelConditions})`);
  }

  if (jobtitles.length > 0) {
    const jobTitleConditions = jobtitles.map(jobtitle => `(job_title LIKE '%${jobtitle}%')`).join(' OR ');
    conditions.push(`(${jobTitleConditions})`);
  }

  if (job_title_role.length > 0) {
    const jobTitleRoleConditions = job_title_role.map(role => `(job_title_role LIKE '%${role}%')`).join(' OR ');
    conditions.push(`(${jobTitleRoleConditions})`);
  }

  if (job_title_sub_role.length > 0) {
    const jobTitleSubRoleConditions = job_title_sub_role.map(subRole => `(job_title_sub_role LIKE '%${subRole}%')`).join(' OR ');
    conditions.push(`(${jobTitleSubRoleConditions})`);
  }

  if (job_company_name.length > 0) {
    const jobCompanyNameConditions = job_company_name.map(job_company_name => `(c.job_company_name LIKE '%${job_company_name}%')`).join(' OR ');
    conditions.push(`(${jobCompanyNameConditions})`);
  }

  if (job_company_website.length > 0) {
    const jobCompanyWebsiteConditions = job_company_website.map(website => `(c.job_company_website LIKE '%${website}%')`).join(' OR ');
    conditions.push(`(${jobCompanyWebsiteConditions})`);
  }

  if (exclude_job_company_name.length > 0) {
    const excludeCompanyNameConditions = exclude_job_company_name.map(exclude_company => `(c.job_company_name NOT LIKE '%${exclude_company}%')`).join(' AND ');
    conditions.push(`(${excludeCompanyNameConditions})`);
  }

  if (exclude_job_company_website.length > 0) {
    const excludeCompanyWebsiteConditions = exclude_job_company_website.map(exclude_website => `(c.job_company_website NOT LIKE '%${exclude_website}%')`).join(' AND ');
    conditions.push(`(${excludeCompanyWebsiteConditions})`);
  }

  if (mobile_phone.length > 0) {
    const mobilePhoneConditions = mobile_phone.map(phone => `(mobile_phone LIKE '%${phone}%')`).join(' OR ');
    conditions.push(`(${mobilePhoneConditions})`);
  }

  if (phone_numbers.length > 0) {
    const phoneNumbersConditions = phone_numbers.map(phone => `(phone_numbers LIKE '%${phone}%')`).join(' OR ');
    conditions.push(`(${phoneNumbersConditions})`);
  }

  if (countries && countries.length > 0) {
    const countryConditions = countries.map(countries => `(countries LIKE '%${countries}%')`).join(' OR ');
    conditions.push(`(${countryConditions})`);
  }



  if (job_company_size && job_company_size.length > 0) {
    const job_company_sizeConditions = job_company_size.map(job_company_size => `(c.job_company_size LIKE '%${job_company_size}%')`).join(' OR ');
    conditions.push(`(${job_company_sizeConditions})`);
  }



  if (job_company_industry && job_company_industry.length > 0) {
    const job_company_industryConditions = job_company_industry.map(type => `(c.job_company_industry LIKE '%${type}%')`).join(' OR ');
    conditions.push(`(${job_company_industryConditions})`);
  }

  if (job_company_type && job_company_type.length > 0) {
    const jobCompanyTypeConditions = job_company_type.map(type => `(c.job_company_type LIKE '%${type}%')`).join(' OR ');
    conditions.push(`(${jobCompanyTypeConditions})`);
  }

  if (linkedin_connections && linkedin_connections.length > 0) {
      conditions.push(`(linkedin_connections LIKE '%${linkedin_connections}%')`);
  }

  if (job_start_date && job_start_date.length > 0) {
        conditions.push(`(YEAR(job_start_date) = ${job_start_date})`);

  }

  if (inferred_years_experience && inferred_years_experience.length > 0) {
    conditions.push(`(inferred_years_experience LIKE '%${inferred_years_experience}%')`);
  }

  if (job_company_founded && job_company_founded.length > 0) {
        conditions.push(`(c.job_company_founded LIKE '%${job_company_founded}%')`);

  }



const startTime = new Date();
const query1 = `DROP TEMPORARY TABLE IF EXISTS temp_result`;
db.query(query1, (err, results) => {
  console.log(query1);  
});
  

  const query = `
     CREATE TEMPORARY TABLE IF NOT EXISTS temp_result AS
  SELECT e.*, c.*
  FROM employees e
  LEFT JOIN company_details c ON e.job_company_id = c.company_id
  WHERE ${conditions.join(' AND ')}

  `;

db.query(query, (err, results) => {
  console.log(query);  
  	res.send(results);
 const endTime = new Date();
      const queryTime = endTime - startTime;
      console.log(`Query execution time: ${queryTime} ms`);
	
	
	
  const tempTableQuery = `SELECT * FROM temp_result`;
  db.query(tempTableQuery, (err, tempResults) => {
    if (err) throw err;
	//res.send(tempResults);
//console.log(tempResults);
    // Convert the results to XLS format
    const ws = XLSX.utils.json_to_sheet(tempResults);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Results');

    // Write the workbook to a file
    const filePath = path.join(__dirname, 'result.xlsx');
    XLSX.writeFile(wb, filePath);

    console.log('result.xlsx file generated');
  });
});



//datatable endpoint 

// Endpoint to fetch data with pagination


//datatable endpoint  ends







});

// export start 
//download enpoint start
app.get('/download', (req, res) => {
  const filePath = path.join(__dirname, 'result.xlsx');
  res.download(filePath, 'result.xlsx', (err) => {
    if (err) {
      console.error('Error downloading the file:', err);
      res.status(500).send('Error downloading the file');
    }
  });
});

// export ends 


// datatbale starts 
app.get('/data', (req, res) => {
  const start = parseInt(req.query.start) || 0;
  const length = parseInt(req.query.length) || 10;

  const tempTableQuery = `
    SELECT SQL_CALC_FOUND_ROWS * 
    FROM temp_result 
    LIMIT ${start}, ${length}
  `;

  db.query(tempTableQuery, (err, tempResults) => {
    if (err) throw err;

    db.query('SELECT FOUND_ROWS() as total', (err, totalResults) => {
      if (err) throw err;

      const totalRecords = totalResults[0].total;
      res.json({
        draw: req.query.draw,
        recordsTotal: totalRecords,
        recordsFiltered: totalRecords,
        data: tempResults
      });
    });
  });
});

// datatable ends 




// Body Parser Middleware
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

// Serve static files from the "public" directory
app.use(express.static('public'));

// File Upload Middleware
app.use(fileUpload());

// Endpoint to handle file upload (POST method)
app.post('/upload', (req, res) => {
  try {
    const chunkNumber = parseInt(req.body.resumableChunkNumber);
    const chunkSize = parseInt(req.body.resumableChunkSize);
    const totalSize = parseInt(req.body.resumableTotalSize);
    const identifier = req.body.resumableIdentifier;
    const filename = req.body.resumableFilename;

    // Directory to store uploaded chunks
    const chunkDir = path.join(__dirname, 'uploads', identifier);
    fs.ensureDirSync(chunkDir);

    // Path to save current chunk
    const chunkPath = path.join(chunkDir, `${filename}-part-${chunkNumber}`);
    req.files.file.mv(chunkPath, (err) => {
      if (err) {
        console.error('Error moving file:', err);
        return res.status(500).send('Error moving file');
      }

      // List all uploaded chunks
      const uploadedChunks = fs.readdirSync(chunkDir).filter(name => name.includes(filename));

      // Check if all chunks have been uploaded
      if (uploadedChunks.length === Math.ceil(totalSize / chunkSize)) {
        // Sort chunks numerically based on part number
        uploadedChunks.sort((a, b) => {
          const aNumber = parseInt(a.split('-part-')[1]);
          const bNumber = parseInt(b.split('-part-')[1]);
          return aNumber - bNumber;
        });

        console.log('3w started');

        // Create a write stream to concatenate chunks into a single file
        const filePath = path.join(__dirname, 'uploads', filename);
        const fileStream = fs.createWriteStream(filePath);

        // Log the variables along with the message
        console.log(`dan dana dan dan chunkDir: ${chunkDir}, uploadedChunks: ${uploadedChunks}, fileStream: ${fileStream.path}`);

      

        fileStream.end();
        console.log('File successfully uploaded and concatenated:', filename);
        console.log('CSVmerger started');

        // Process the concatenated CSV file
        processCSV(filePath).then(() => {
          // Send success response to client
          res.status(200).send('File successfully uploaded and processed');
        }).catch(err => {
          console.error('Error processing CSV:', err);
          res.status(500).send('Error processing CSV');
        });
      } else {
        console.log('1 started');
        // More chunks to upload, send acknowledgment
        res.status(200).send('Chunk uploaded');
        console.log('3 started');
      }
    });
  } catch (err) {
    console.error('Error uploading file:', err);
    res.status(500).send('Error uploading file');
  }
});

// Function to concatenate chunks
async function concatenateChunks(chunkDir, uploadedChunks, fileStream) {
  for (let i = 0; i < uploadedChunks.length; i++) {
    const chunkFilePath = path.join(chunkDir, uploadedChunks[i]);
    const chunk = fs.readFileSync(chunkFilePath);
    fileStream.write(chunk);
    fs.unlinkSync(chunkFilePath); // Delete chunk file after concatenating
  }
  	 console.log('we3 started');

}


// Function to process the CSV file and insert into database
async function processCSV(filePath) {
  try {
    const stream = fs.createReadStream(filePath);
    let csvData = [];
    let rowCount = 0;

    await new Promise((resolve, reject) => {
      csv.parseStream(stream, { headers: true, ignoreEmpty: true, trim: true })
        .on('error', err => {
          console.error('Parsing error:', err);
          reject(err);
        })
        .on('data-invalid', (row, rowIndex, err) => {
          console.error('Invalid row encountered:', rowIndex, row, err);
        })
        .on('data', async row => {
          try {
            csvData.push(row);
            rowCount++;

            if (rowCount === 1000) {
              await insertDataIntoDatabase(csvData);
              csvData = [];
              rowCount = 0;
            }
          } catch (err) {
            console.error('Error processing row:', row, err);
          }
        })
        .on('end', async () => {
          if (csvData.length > 0) {
            await insertDataIntoDatabase(csvData);
          }
          resolve();
        });
    });

    console.log('CSV data processed and inserted into database');
  } catch (err) {
    console.error('Error processing CSV:', err);
    throw err;
  }
}


// Endpoint to handle all chunks uploaded notification
app.post('/allclusteruploaded', (req, res) => {
  const { identifier, filename } = req.body;

  // Directory to store uploaded chunks
  const chunkDir = path.join(__dirname, 'uploads', identifier);

  // List all uploaded chunks
  const uploadedChunks = fs.readdirSync(chunkDir).filter(name => name.includes(filename));

  // Create a write stream to concatenate chunks into a single file
  const filePath = path.join(__dirname, 'uploads', filename);
  const fileStream = fs.createWriteStream(filePath);

  // Log the variables along with the message
 // console.log(`dan dana dan dan chunkDir: ${chunkDir}, uploadedChunks: ${uploadedChunks}, fileStream: ${fileStream.path}`);


    // Concatenate chunks into the single file
       concatenateChunks(chunkDir, uploadedChunks, fileStream);
		
		
		  setTimeout(() => {

 // Process the concatenated CSV file
       processCSV(filePath).then(() => {
          // Send success response to client
          res.status(200).send('File successfully uploaded and processed');
        }).catch(err => {
          console.error('Error processing CSV:', err);
          res.status(500).send('Error processing CSV');
        });
      }, 10000); // 3000 milliseconds (3 seconds) delay
		  //res.status(200).send('All chunks uploaded notification received');
});



// Function to insert data into MySQL database
async function insertDataIntoDatabase(data) {
  try {
    // Define the SQL query for batch insertion
    const employeeInsertQuery = `
      INSERT INTO temp_employees (
        filter_id, full_name, first_name, middle_initial, middle_name, last_name, gender, birth_year, birth_date, linkedin_url, linkedin_username, linkedin_id, facebook_url, facebook_username, facebook_id, twitter_url, twitter_username, github_url, github_username, work_email, mobile_phone, industry, job_title, job_title_role, job_title_sub_role, job_title_levels, job_company_id, job_company_name, job_company_website, job_company_size, job_company_founded, job_company_industry, job_company_linkedin_url, job_company_linkedin_id, job_company_facebook_url, job_company_twitter_url, job_company_location_name, job_company_location_locality, job_company_location_metro, job_company_location_region, job_company_location_geo, job_company_location_street_address, job_company_location_address_line_2, job_company_location_postal_code, job_company_location_country, job_company_location_continent, job_last_updated, job_start_date, job_summary, location_name, location_locality, location_metro, location_region, location_country, location_continent, location_street_address, location_address_line_2, location_postal_code, location_geo, location_last_updated, linkedin_connections, inferred_salary, inferred_years_experience, summary, phone_numbers, emails, interests, skills, location_names, regions, countries, street_addresses, experience, education, profiles, certifications, languages, version_status
      ) VALUES ?
    `;

    // Map each row of data to match the database schema
    const employeeData = data.map(row => [
      row.id, row.full_name, row.first_name, row.middle_initial, row.middle_name, row.last_name, row.gender, row.birth_year, row.birth_date, row.linkedin_url, row.linkedin_username, row.linkedin_id, row.facebook_url, row.facebook_username, row.facebook_id, row.twitter_url, row.twitter_username, row.github_url, row.github_username, row.work_email, row.mobile_phone, row.industry, row.job_title, row.job_title_role, row.job_title_sub_role, row.job_title_levels, row.job_company_id, row.job_company_name, row.job_company_website, row.job_company_size, row.job_company_founded, row.job_company_industry, row.job_company_linkedin_url, row.job_company_linkedin_id, row.job_company_facebook_url, row.job_company_twitter_url, row.job_company_location_name, row.job_company_location_locality, row.job_company_location_metro, row.job_company_location_region, row.job_company_location_geo, row.job_company_location_street_address, row.job_company_location_address_line_2, row.job_company_location_postal_code, row.job_company_location_country, row.job_company_location_continent, row.job_last_updated, row.job_start_date, row.job_summary, row.location_name, row.location_locality, row.location_metro, row.location_region, row.location_country, row.location_continent, row.location_street_address, row.location_address_line_2, row.location_postal_code, row.location_geo, row.location_last_updated, row.linkedin_connections, row.inferred_salary, row.inferred_years_experience, row.summary, row.phone_numbers, row.emails, row.interests, row.skills, row.location_names, row.regions, row.countries, row.street_addresses, row.experience, row.education, row.profiles, row.certifications, row.languages, row.version_status
    ]);

    await db.promise().query(employeeInsertQuery, [employeeData]);
    console.log('Data inserted into database');
  } catch (err) {
    console.error('Error inserting data into database:', err);
    throw err; // Propagate error to caller
  }
}

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});