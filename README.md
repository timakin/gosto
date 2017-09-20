Gosto
----

Cloud Datastore Client Wrapper in Go, for automation of key attachment.

For example, inside of GKE container, you can access to datastore API of you're authorized with Service Account.

## Table of Contents

<!-- MarkdownTOC -->

- [Supported API](#supported-api)
- [Using the library](#using-the-library)
  - [Quick start](#quick-start)
  - [Get](#get)
  - [GetMulti](#get-multi)
  - [Delete](#delete)
  - [DeleteMulti](#delete-multi)
  - [Put](#put)
  - [PutMulti](#put-multi)
  - [RunInTransaction](#run-in-transaction)
  
<!-- /MarkdownTOC -->

## Supported API

- Get
- GetMulti
- Delete
- DeleteMulti
- Put
- PutMulti
- RunInTransaction

## Using the library

### Quick Start

A client of gosto can be used with `context.Context` and Google Cloud Platform's `project-id`.

```
client, err := gosto.NewGosto(ctx, "project-id")
```

### Get

```
client, err := gosto.NewGosto(ctx, "project-id")
if err != nil {
    ...
}

user := &User{
    ID: 1,
}
if err = client.Get(user); err != nil {
    ...
}
```

### GetMulti

```
client, err := gosto.NewGosto(ctx, "project-id")
if err != nil {
    ...
}

users := []*User{
    &User{
        ID: 1,
    },
    &User{
        ID: 2,
    },
}
if err = client.GetMulti(users); err != nil {
    ...
}
```

### Delete

```
client, err := gosto.NewGosto(ctx, "project-id")
if err != nil {
    ...
}

user := &User{
    ID: 1,
    ...
}

key := client.Key(user)
if err = client.Delete(key); err != nil {
    ...
}
```

### DeleteMulti

```
client, err := gosto.NewGosto(ctx, "project-id")
if err != nil {
    ...
}

users := []*User{
    &User{
        ID: 1,
    },
    &User{
        ID: 2,
    },
}

var keys []*datastore.Key
for i := range users {
    user := users[i]
    keys = append(keys, client.Key(user))
}

if err = client.DeleteMulti(keys); err != nil {
    ...
}
```

### Put

```
client, err := gosto.NewGosto(ctx, "project-id")
if err != nil {
    ...
}

user := &User{
    Name: "John Doe",
}

if key, err = client.Put(user); err != nil {
    ...
}
```

### PutMulti

```
client, err := gosto.NewGosto(ctx, "project-id")
if err != nil {
    ...
}

users := []*User{
    &User{
        Name: "John Doe",
    },
    &User{
        Name: "Jane Doe",
    },
}


if keys, err = client.PutMulti(users); err != nil {
    ...
}
```

### RunInTransaction

```
client, err := gosto.NewGosto(ctx, "project-id")
if err != nil {
    ...
}

if err = client.RunInTransaction(func(tx *datastore.Transaction) error {
    user := &User{
        Name: "John Doe",
    }
    uKey := client.Key(user)
	if err := tx.Put(uKey, user); err != nil && err != datastore.ErrNoSuchEntity {
		return err
	}
    prof := &UserProfile{
        UserID: user.ID,
        Content: "HogeFuga",
    }
    pKey := client.Key(prof)
	if _, err := tx.Put(pKey, prof); err != nil {
		return err
	}
	return nil
})
```